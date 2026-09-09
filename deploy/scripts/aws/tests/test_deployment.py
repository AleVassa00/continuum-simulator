"""Optional real Bash/Compose checks; no containers or EC2 resources are started.

Set BASH_BIN and DEPLOYGEN_BIN to enable these integration tests.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from test_artifacts import budget


REPO = Path(__file__).resolve().parents[4]
BASH = os.environ.get("BASH_BIN")
DEPLOYGEN = os.environ.get("DEPLOYGEN_BIN")


@unittest.skipUnless(BASH, "Set BASH_BIN for Bash integration tests")
class ShellTests(unittest.TestCase):
    def shell(self, command, **env):
        return subprocess.run([BASH, "-c", command], cwd=REPO,
                              env={**os.environ, **env}, capture_output=True, text=True)

    def test_all_scripts_parse(self):
        for path in (REPO / "deploy/scripts").rglob("*.sh"):
            with self.subTest(script=path.name):
                result = subprocess.run([BASH, "-n", path.as_posix()], capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_profile_load_and_source_fingerprint(self):
        result = self.shell('source deploy/scripts/aws/prepare-pilot.sh; load_resource_profile; '
                            'printf "%s" "$RESOURCE_PROFILE_VALUES"; calculate_source_sha256')
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 15)
        self.assertIn("CLOUD_WORKER_CPUS=0.25", lines)
        self.assertRegex(lines[-1], r"^[0-9a-f]{64}$")

    def test_invalid_profiles_rejected_without_execution(self):
        original = (REPO / "deploy/resources/aws-pilot.env").read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.env"
            for extra in ("CLOUD_WORKER_CPUS=0.5\n", "UNKNOWN_CPUS=1\n", "$(exit 0)\n"):
                with self.subTest(extra=extra):
                    path.write_text(original + "\n" + extra, encoding="utf-8")
                    result = self.shell('source deploy/scripts/aws/prepare-pilot.sh; load_resource_profile',
                                        RESOURCE_PROFILE=path.as_posix())
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("resource profile", result.stderr)
            path.write_text(original.replace("EDGE_MEMORY=64m", ""), encoding="utf-8")
            result = self.shell('source deploy/scripts/aws/prepare-pilot.sh; load_resource_profile',
                                RESOURCE_PROFILE=path.as_posix())
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("manca EDGE_MEMORY", result.stderr)

    def test_collector_once_and_failed_query(self):
        # Exported functions mock only the external Docker/timeout commands.
        command = '''
docker() {
  local group="${!#}" topic count
  if [[ "$group" == cloud-workers ]]; then topic=edge-aggregates; count=6;
  else topic=cloud-partition-aggregates; count=1; fi
  for ((p=0; p<count; p++)); do echo "$group $topic $p 10 10 0 consumer host client"; done
  return "${MOCK_QUERY_EXIT:-0}"
}
timeout() { shift; "$@"; }
export -f docker timeout
bash deploy/scripts/aws/collect-kafka-lag.sh 5 offline-test once
'''
        from test_artifacts import artifacts
        result = self.shell(command)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(artifacts.parse_lag(result.stdout)), 7)
        result = self.shell(command, MOCK_QUERY_EXIT="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(artifacts.parse_lag(result.stdout), [])
        result = self.shell('bash deploy/scripts/aws/collect-kafka-lag.sh 5 offline-test invalid')
        self.assertEqual(result.returncode, 2)

    def test_runner_normalized_compose_keeps_replay_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "compose").mkdir()
            command = '''
source deploy/scripts/aws/run-experiment.sh
ARTIFACT_DIR="$MOCK_ROOT"
PUBLIC_IPS[simulator]=offline-host
ssh_run() { shift; "$@"; }
cd() { builtin cd "$MOCK_ROOT"; }
docker() { printf '{"replay_start_at":"%s","args":"%s"}\\n' "$REPLAY_START_AT" "$*"; }
export -f cd docker
collect_normalized_compose simulator
REPLAY_START_AT=2026-09-07T12:34:56Z
collect_normalized_compose simulator .run.env
'''
            result = self.shell(command, MOCK_ROOT=root.as_posix())
            self.assertEqual(result.returncode, 0, result.stderr)
            for extension in ("json", "yml"):
                config = json.loads((root / "compose" / f"simulator.normalized.{extension}").read_text())
                self.assertEqual(config["replay_start_at"], "2026-09-07T12:34:56Z")
                self.assertIn("--env-file .run.env --profile replay", config["args"])

    def test_runner_propagates_postprocessing_exit_codes(self):
        from test_artifacts import fixture
        for expected_code in (0, 1, 2):
            with self.subTest(exit_code=expected_code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                fixture(root)
                if expected_code == 1:
                    (root / "logs/simulator.log").write_text("", encoding="utf-8")
                elif expected_code == 2:
                    (root / "metrics/kafka-lag.log").write_text("", encoding="utf-8")
                # Only external collection/metadata commands are mocked. The
                # real finalizer invokes the real parser and propagates its code.
                command = '''
source deploy/scripts/aws/run-experiment.sh
ARTIFACT_DIR="$MOCK_ROOT"
capture_container_states() { :; }
collect_host_logs() { :; }
write_run_metadata() { :; }
jq() { command cat "${!#}"; }
finalize_run
'''
                result = self.shell(command, MOCK_ROOT=root.as_posix(), PYTHON_BIN=Path(sys.executable).as_posix())
                self.assertEqual(result.returncode, expected_code, result.stderr + result.stdout)


@unittest.skipUnless(DEPLOYGEN and shutil.which("docker"), "Set DEPLOYGEN_BIN and provide Docker Compose")
class ComposeTests(unittest.TestCase):
    def test_real_generated_configs_for_1_2_4_6_workers(self):
        original = (REPO / "experiments/calibration-aws.yaml").read_text()
        common_configs = {}
        for workers in (1, 2, 4, 6):
            with self.subTest(workers=workers), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "dataset/output").mkdir(parents=True)
                shutil.copyfile(REPO / "dataset/output/kmeans_topology.csv", root / "dataset/output/kmeans_topology.csv")
                (root / "experiment.yaml").write_text(original.replace("workers: 1", f"workers: {workers}"), encoding="utf-8")
                result = subprocess.run([DEPLOYGEN, "-mode", "distributed", "-experiment", "experiment.yaml"],
                                        cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                manifest = json.loads((root / "deploy/compose/distributed/generation-manifest.json").read_text())
                env = {**os.environ, "DEPLOYMENT_ID": "offline-check", "EDGE_HOST": "10.0.0.2",
                       "CLOUD_KAFKA_HOST": "10.0.0.3", "KAFKA_ADVERTISED_HOST": "10.0.0.3",
                       "REPLAY_START_AT": "2026-09-07T00:00:00Z"}
                for role, count in (("simulator", 13), ("edge", 26), ("cloud-core", 3), ("workers", workers)):
                    filename = f"{role}.generated.yml"
                    path = root / "deploy/compose/distributed" / filename
                    self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), manifest["compose_sha256"][filename])
                    command = ["docker", "compose", "--env-file", str(REPO / "deploy/resources/aws-pilot.env"),
                               "--profile", "replay", "-f", str(path), "config", "--format", "json"]
                    result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    config = json.loads(result.stdout)
                    self.assertEqual(len(config["services"]), count)
                    capacity = {"cpus": 2, "memory_bytes": (4 if role == "cloud-core" else 2) * 1024**3}
                    budget.check(config, capacity)
                    # Compare workload, service limits, topics and Cloud windows
                    # while ignoring only temporary bind-mount source directories.
                    normalized = json.dumps(config["services"], sort_keys=True).replace(
                        json.dumps(str(root))[1:-1], "<ROOT>")
                    if role == "workers":
                        for service in config["services"].values():
                            self.assertEqual(float(service["cpus"]), 0.25)
                            self.assertEqual(int(service["mem_limit"]), 128 * 1024**2)
                    elif role in common_configs:
                        self.assertEqual(normalized, common_configs[role])
                    else:
                        common_configs[role] = normalized


if __name__ == "__main__":
    unittest.main()
