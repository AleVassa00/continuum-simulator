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

    def test_runner_checks_configured_topic_count_exactly(self):
        command = '''
KAFKA_READY_TIMEOUT_SECONDS=2
POLL_INTERVAL_SECONDS=1
source deploy/scripts/aws/run-experiment.sh
PUBLIC_IPS[cloud-core]=local
PUBLIC_IPS[workers]=local
CONFIGURED_PARTITIONS=8
# Exercise the topic-check scripts; other remote deployment actions are mocked.
ssh_run() { shift; if [[ "$1" == bash ]]; then "$@"; else return 0; fi; }
docker() {
  if [[ "$1" == inspect ]]; then
    case "$3" in
      *Health*) echo healthy ;;
      *ExitCode*) echo 0 ;;
      *) if [[ "$4" == kafka-init ]]; then echo exited; else echo running; fi ;;
    esac
  elif [[ "$*" == *'--topic edge-aggregates'* ]]; then
    echo "Topic: edge-aggregates PartitionCount: $MOCK_PARTITIONS ReplicationFactor: 1"
  else
    echo "Topic: cloud-partition-aggregates PartitionCount: 1 ReplicationFactor: 1"
  fi
}
export -f docker
collect_normalized_compose() { :; }
verify_kafka_tcp_from_role() { :; }
wait_for_worker_group() { :; }
wait_for_edges() { :; }
wait_for_time_sync() { :; }
coordinator_status() { :; }
wait_for_kafka
quick_preflight
'''
        result = self.shell(command, MOCK_PARTITIONS="8")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        for count in ("6", "80"):
            result = self.shell(command, MOCK_PARTITIONS=count)
            self.assertNotEqual(result.returncode, 0, result.stderr + result.stdout)

    def test_profile_load_and_source_fingerprint(self):
        result = self.shell('source deploy/scripts/aws/prepare-pilot.sh; load_resource_profile; '
                            'printf "%s" "$RESOURCE_PROFILE_VALUES"; calculate_source_sha256')
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 17)
        self.assertIn("CLOUD_WORKER_CPUS=0.25", lines)
        self.assertIn("COORDINATOR_CPUS=0.1", lines)
        self.assertIn("COORDINATOR_MEMORY=64m", lines)
        self.assertRegex(lines[-1], r"^[0-9a-f]{64}$")

    @unittest.skipUnless(shutil.which("jq"), "Install jq for RDS environment checks")
    def test_rds_environment_only_reaches_cloud_core(self):
        command = '''
source deploy/scripts/aws/prepare-pilot.sh
terraform_output() { echo '{"host":"db.example.rds.amazonaws.com","port":5432,"database":"continuum","username":"continuum_admin"}'; }
load_rds_configuration
load_resource_profile
PRIVATE_IPS[cloud-core]=10.0.0.1
PRIVATE_IPS[edge]=10.0.0.2
STAGING_ROOT=$(mktemp -d)
for role in "${ROLES[@]}"; do
  mkdir "$STAGING_ROOT/$role"
  write_runtime_environment "$role" "$STAGING_ROOT/$role"
  if [[ "$role" == cloud-core ]]; then
    grep -Fx 'GLOBAL_SINK_TYPE=postgres' "$STAGING_ROOT/$role/.env"
    grep -Fx 'GLOBAL_POSTGRES_SSLMODE=verify-full' "$STAGING_ROOT/$role/.env"
    grep -Fx 'GLOBAL_POSTGRES_HOST=db.example.rds.amazonaws.com' "$STAGING_ROOT/$role/.env"
    grep -Fx 'GLOBAL_POSTGRES_PORT=5432' "$STAGING_ROOT/$role/.env"
    grep -Fx 'GLOBAL_POSTGRES_DATABASE=continuum' "$STAGING_ROOT/$role/.env"
    grep -Fx 'GLOBAL_POSTGRES_USER=continuum_admin' "$STAGING_ROOT/$role/.env"
    grep -Fx "GLOBAL_POSTGRES_PASSWORD=$TF_VAR_rds_password" "$STAGING_ROOT/$role/.env" >/dev/null
  else
    ! grep -q GLOBAL_POSTGRES "$STAGING_ROOT/$role/.env"
  fi
done
'''
        result = self.shell(command, TF_VAR_rds_password="OnlyForTesting_123!")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("OnlyForTesting_123!", result.stdout + result.stderr)
        result = self.shell(command, TF_VAR_rds_password="")
        self.assertNotEqual(result.returncode, 0)

    def test_rds_schema_installer_branches_and_failures(self):
        command = '''
docker() {
  if [[ "$*" == *' ps '* ]]; then
    [[ "$MOCK_SCHEMA" != running ]] || echo global-id
    return 0
  fi
  while [[ "$1" != global-aggregator ]]; do shift; done
  shift
  bash "$@"
}
psql() {
  if [[ "$*" == *to_regclass* ]]; then
    case "$MOCK_SCHEMA" in
      unavailable) return 1 ;;
      absent|install-fails) echo f ;;
      *) echo t ;;
    esac
  elif [[ "$*" == *' -f '* ]]; then
    [[ "$MOCK_SCHEMA" != install-fails ]] || return 1
    echo installed
  else
    [[ "$MOCK_SCHEMA" != legacy ]] || return 1
  fi
}
export -f docker psql
export GLOBAL_SINK_TYPE=postgres GLOBAL_POSTGRES_HOST=db.example GLOBAL_POSTGRES_PORT=5432
export GLOBAL_POSTGRES_DATABASE=continuum GLOBAL_POSTGRES_USER=continuum_admin
export GLOBAL_POSTGRES_PASSWORD=OnlyForTesting_123 GLOBAL_POSTGRES_SSLMODE=verify-full
bash deploy/scripts/aws/init-rds-schema.sh
'''
        for state in ("absent", "existing", "legacy", "unavailable", "install-fails", "running"):
            with self.subTest(state=state):
                result = self.shell(command, MOCK_SCHEMA=state)
                self.assertEqual(result.returncode == 0, state in ("absent", "existing"), result.stderr)
                self.assertNotIn("OnlyForTesting_123", result.stdout + result.stderr)
                if state == "existing":
                    self.assertIn("data preserved", result.stdout)

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

    def test_coordinator_profile_overrides(self):
        original = (REPO / "deploy/resources/aws-pilot.env").read_text()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.env"
            path.write_text(original + "\nCOORDINATOR_CPUS=0.15\nCOORDINATOR_MEMORY=96m\n", encoding="utf-8")
            result = self.shell('source deploy/scripts/aws/prepare-pilot.sh; load_resource_profile; '
                                'printf "%s" "$RESOURCE_PROFILE_VALUES"', RESOURCE_PROFILE=path.as_posix())
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("COORDINATOR_CPUS=0.15", result.stdout)
            self.assertIn("COORDINATOR_MEMORY=96m", result.stdout)

    def test_collector_once_and_failed_query(self):
        # Mock the Docker boundary, not the launcher's lifecycle or output.
        command = '''
docker() {
  echo "docker $*" >&2
  case "$1" in
    inspect)
      if [[ "$3" == '{{.Image}}' ]]; then echo sha256:prepared-image;
      else echo "${MOCK_QUERY_EXIT:-0}"; fi ;;
    create) echo helper-id ;;
    start)
      for group in cloud-workers global-aggregator; do
        echo "===== group $group sample 2026-09-09T19:00:00Z ====="
        if [[ "$group" == cloud-workers ]]; then topic=edge-aggregates; count=6;
        else topic=cloud-partition-aggregates; count=1; fi
        if [[ "${MOCK_QUERY_EXIT:-0}" == 0 ]]; then
          for ((p=0; p<count; p++)); do echo "$group $topic $p 10 10 0 - - -"; done
        fi
        echo "===== query_exit ${MOCK_QUERY_EXIT:-0} at 2026-09-09T19:00:01Z ====="
      done ;;
    rm) echo removed-helper >&2 ;;
    *) return 99 ;;
  esac
}
export -f docker
bash deploy/scripts/aws/collect-kafka-lag.sh 5 offline-test once 6
'''
        from test_artifacts import artifacts
        result = self.shell(command)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(artifacts.parse_lag(result.stdout, 6)), 7)
        self.assertEqual(result.stderr.count("docker create "), 1)
        self.assertIn("--network container:kafka --cpus 0.05", result.stderr)
        self.assertIn("sha256:prepared-image -broker localhost:29092 -interval 5s -mode once", result.stderr)
        self.assertNotIn("docker exec", result.stderr)
        self.assertIn("--env SOURCE_PARTITION_COUNT=6", result.stderr)
        result = self.shell(command, MOCK_QUERY_EXIT="1")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(artifacts.parse_lag(result.stdout, 6), [])
        result = self.shell('bash deploy/scripts/aws/collect-kafka-lag.sh 5 offline-test invalid')
        self.assertEqual(result.returncode, 2)

    def test_collector_cleanup_on_signal(self):
        command = '''
set -eu
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
export MOCK_TRACE="$scratch/trace" MOCK_READY="$scratch/ready" MOCK_STOP="$scratch/stop"
docker() {
  echo "$1" >>"$MOCK_TRACE"
  case "$1" in
    inspect) echo sha256:prepared-image ;;
    create) echo helper-id ;;
    start)
      touch "$MOCK_READY"
      while [[ ! -e "$MOCK_STOP" ]]; do sleep 0.05; done ;;
    rm) [[ "$*" == 'rm -f helper-id' ]]; touch "$MOCK_STOP" ;;
    *) return 99 ;;
  esac
}
export -f docker
bash deploy/scripts/aws/collect-kafka-lag.sh 5 "offline-signal-$$" loop 6 &
collector=$!
for ((attempt=0;attempt<100;attempt++)); do
  [[ ! -e "$MOCK_READY" ]] || break
  sleep 0.05
done
[[ -e "$MOCK_READY" ]]
kill -TERM "$collector"
wait "$collector"
[[ ! -e "/tmp/continuum-kafka-lag-offline-signal-$$.pid" ]]
cat "$MOCK_TRACE"
'''
        result = self.shell(command)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["inspect", "create", "start", "rm"])

    def test_runner_edge_lifecycle(self):
        command = '''
source deploy/scripts/aws/run-experiment.sh
WORKER_COUNT=1
for role in "${ROLES[@]}"; do PUBLIC_IPS["$role"]=offline-host; done
ssh_run() { shift; bash -c "$*"; }
capture_container_states() { :; }
docker() {
  local name="${!#}" field="$3" state=running health=healthy
  [[ "$name" != kafka-init ]] || state=exited
  if [[ "$MOCK_PHASE" == after ]]; then
    case "$name" in edge-*|simulator-*|global-aggregator) state=exited; health=unhealthy ;; esac
  fi
  if [[ "$name" == "${MOCK_TARGET:-edge-0}" && "$field" == "${MOCK_FIELD:-}" ]]; then
    echo "$MOCK_VALUE"; return
  fi
  case "$field" in
    '{{.State.Status}}') echo "$state" ;;
    '{{.RestartCount}}'|'{{.State.ExitCode}}') echo 0 ;;
    '{{.State.OOMKilled}}') echo false ;;
    *) echo "$health" ;;
  esac
}
export -f docker
validate_container_lifecycle "$MOCK_PHASE"
'''
        for phase in ("before", "after"):
            result = self.shell(command, MOCK_PHASE=phase)
            self.assertEqual(result.returncode, 0, result.stderr)
        for phase, target, field, value in (
            ("before", "edge-0", "{{.State.Status}}", "exited"),
            ("after", "edge-0", "{{.State.Status}}", "running"),
            ("after", "edge-0", "{{.State.ExitCode}}", "1"),
            ("after", "edge-0", "{{.RestartCount}}", "1"),
            ("after", "edge-0", "{{.State.OOMKilled}}", "true"),
            ("after", "mqtt-edge-0", "{{.State.Status}}", "exited"),
            ("before", "partition-coordinator", "{{.State.Status}}", "exited"),
            ("after", "partition-coordinator", "{{.RestartCount}}", "1"),
            ("after", "partition-coordinator", "{{.State.OOMKilled}}", "true"),
            ("after", "partition-coordinator", "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", "unhealthy"),
        ):
            with self.subTest(phase=phase, field=field, value=value):
                result = self.shell(command, MOCK_PHASE=phase, MOCK_TARGET=target,
                                    MOCK_FIELD=field, MOCK_VALUE=value)
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertIn(target, result.stderr)

    def test_runner_normalized_compose_keeps_replay_timestamp(self):
        for role in ("cloud-core", "workers", "edge", "simulator"):
            for env_file, replay_start in ((".env", ""), (".run.env", "2026-09-07T12:34:56Z")):
                with self.subTest(role=role, env_file=env_file), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    (root / "compose").mkdir()
                    command = '''
source deploy/scripts/aws/run-experiment.sh
ARTIFACT_DIR="$MOCK_ROOT"
PUBLIC_IPS["$MOCK_ROLE"]=offline-host
# OpenSSH joins command arguments into a string parsed by the remote shell.
# Unlike direct "$@" execution, this really loses an unquoted empty argument.
ssh_run() { shift; bash -c "$*"; }
cd() { builtin cd "$MOCK_ROOT"; }
docker() { printf '{"replay_start_at":"%s","args":"%s","password":"%s"}\\n' "$REPLAY_START_AT" "$*" "$GLOBAL_POSTGRES_PASSWORD"; }
export -f cd docker
REPLAY_START_AT="$MOCK_REPLAY_START_AT"
collect_normalized_compose "$MOCK_ROLE" "$MOCK_ENV_FILE"
'''
                    result = self.shell(command, MOCK_ROOT=root.as_posix(), MOCK_ROLE=role,
                                        MOCK_ENV_FILE=env_file, MOCK_REPLAY_START_AT=replay_start)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    for extension in ("json", "yml"):
                        config = json.loads((root / "compose" / f"{role}.normalized.{extension}").read_text())
                        self.assertEqual(config["password"], "__REDACTED__")
                        self.assertEqual(config["replay_start_at"], replay_start or "1970-01-01T00:00:00Z")
                        expected_args = ["compose", "--env-file", env_file]
                        if role == "simulator":
                            expected_args += ["--profile", "replay"]
                        expected_args += ["-f", f"deploy/compose/distributed/{role}.generated.yml", "config"]
                        if extension == "json":
                            expected_args += ["--format", "json"]
                        self.assertEqual(config["args"].split(), expected_args)

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
    def test_nondefault_partition_count_in_normalized_compose(self):
        original = (REPO / "experiments/calibration-aws.yaml").read_text()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "dataset/output").mkdir(parents=True)
            (root / "dataset/output/kmeans_topology.csv").write_text("sensor_id,edge_id\nsensor-0,edge-0\n", encoding="utf-8")
            (root / "experiment.yaml").write_text(original.replace("partitions: 6", "partitions: 8"), encoding="utf-8")
            result = subprocess.run([DEPLOYGEN, "-mode", "distributed", "-experiment", "experiment.yaml"], cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            env = {**os.environ, "DEPLOYMENT_ID": "offline-check", "EDGE_HOST": "10.0.0.2",
                   "CLOUD_KAFKA_HOST": "10.0.0.3", "KAFKA_ADVERTISED_HOST": "10.0.0.3",
                   "REPLAY_START_AT": "2026-09-07T00:00:00Z"}
            for role in ("edge", "cloud-core", "workers"):
                path = root / "deploy/compose/distributed" / f"{role}.generated.yml"
                result = subprocess.run(["docker", "compose", "--env-file", str(REPO / "deploy/resources/aws-pilot.env"),
                    "-f", str(path), "config", "--format", "json"], cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                services = json.loads(result.stdout)["services"]
                for name, service in services.items():
                    if name in ("partition-coordinator", "global-aggregator") or name.startswith("cloud-worker-"):
                        self.assertEqual(service["environment"]["SOURCE_PARTITION_COUNT"], "8")
                    if name == "kafka-init":
                        self.assertIn("--partitions 8", " ".join(service["command"]))
                        # Compose config escapes literal dollars for reserialization.
                        self.assertIn("PartitionCount: 8([[:space:]]|$)", " ".join(service["command"]).replace("$$", "$"))

    def test_real_generated_configs_for_1_2_4_6_workers(self):
        original = (REPO / "experiments/calibration-aws.yaml").read_text()
        common_configs = {}
        for workers in (1, 2, 4, 6):
            with self.subTest(workers=workers), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "dataset/output").mkdir(parents=True)
                # Synthetic topology keeps deployment tests independent of datasets.
                (root / "dataset/output/kmeans_topology.csv").write_text(
                    "sensor_id,edge_id\n" + "".join(f"sensor-{i},edge-{i}\n" for i in range(13)), encoding="utf-8")
                (root / "experiment.yaml").write_text(original.replace("workers: 1", f"workers: {workers}"), encoding="utf-8")
                result = subprocess.run([DEPLOYGEN, "-mode", "distributed", "-experiment", "experiment.yaml"],
                                        cwd=root, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                manifest = json.loads((root / "deploy/compose/distributed/generation-manifest.json").read_text())
                env = {**os.environ, "DEPLOYMENT_ID": "offline-check", "EDGE_HOST": "10.0.0.2",
                       "CLOUD_KAFKA_HOST": "10.0.0.3", "KAFKA_ADVERTISED_HOST": "10.0.0.3",
                       "REPLAY_START_AT": "2026-09-07T00:00:00Z"}
                for role, count in (("simulator", 13), ("edge", 27), ("cloud-core", 3), ("workers", workers)):
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
