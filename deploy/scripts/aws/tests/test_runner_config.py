"""Offline runner configuration tests: never load real credentials or contact AWS."""
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[4]
BASH = os.environ.get("BASH_BIN")


@unittest.skipUnless(BASH, "Set BASH_BIN for runner configuration tests")
class RunnerConfigTests(unittest.TestCase):
    def shell(self, code, **env):
        return subprocess.run([BASH, "-c", code], cwd=REPO,
                              env={**os.environ, "CONTINUUM_ENV_LOADED": "", **env},
                              text=True, capture_output=True)

    def test_configuration_precedence_and_children(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, content in (
                ("aws.env", "SSH_USER=legacy\nAWS_REGION=us-east-1\n"),
                ("pilot.env", "SSH_USER=pilot\nRESOURCE_PROFILE=deploy/resources/aws-pilot.env\n"),
                ("secrets.env", "SSH_USER=secret\nTF_VAR_rds_password=OnlyForTesting_123\n"),
            ):
                (root / name).write_text(content, encoding="utf-8")
            result = self.shell('''
source deploy/scripts/aws/pilot-env.sh
load_pilot_environment "$PWD"
[[ "$SSH_USER" == explicit ]]
[[ "$AWS_REGION" == us-east-1 ]]
[[ "$RESOURCE_PROFILE" == "$PWD/deploy/resources/aws-pilot.env" ]]
[[ "$TF_VAR_rds_password" == OnlyForTesting_123 ]]
SSH_USER=child
load_pilot_environment "$PWD"
[[ "$SSH_USER" == child ]]
''', AWS_SESSION_FILE=(root / "aws.env").as_posix(),
                CONTINUUM_PILOT_FILE=(root / "pilot.env").as_posix(),
                CONTINUUM_SECRETS_FILE=(root / "secrets.env").as_posix(), SSH_USER="explicit")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("OnlyForTesting", result.stdout + result.stderr)

    def test_missing_explicit_file_is_rejected(self):
        result = self.shell('source deploy/scripts/aws/pilot-env.sh; load_pilot_environment "$PWD"',
                            AWS_SESSION_FILE="/definitely-missing-continuum.env")
        self.assertNotEqual(result.returncode, 0)

    def test_prepare_does_not_inherit_secret_umask(self):
        result = self.shell('umask 077; source deploy/scripts/aws/prepare-pilot.sh; umask')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "0022")

    @unittest.skipUnless(shutil.which("jq"), "Install jq for export checks")
    def test_export_is_promoted_only_after_success(self):
        for fail in ("0", "1"):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "compose").mkdir()
                (root / "compose/cloud-core.normalized.json").write_text(
                    '{"services":{"global-aggregator":{"environment":{"GLOBAL_SINK_TYPE":"postgres"}}}}',
                    encoding="utf-8")
                result = self.shell('''
source deploy/scripts/aws/run-experiment.sh
ARTIFACT_DIR="$MOCK_ROOT"
PUBLIC_IPS[cloud-core]=mock-host
ssh_run() { echo '{}'; return "$MOCK_FAIL"; }
export_postgres_results
''', MOCK_ROOT=root.as_posix(), MOCK_FAIL=fail)
                self.assertEqual(result.returncode, int(fail), result.stderr)
                self.assertEqual((root / "global-aggregates.ndjson").exists(), fail == "0")

    def test_wrapper_modes_and_terraform_override(self):
        for mode in ("", "--reuse-release", "--provision"):
            result = self.shell('''
source deploy/scripts/aws/run-full.sh
load_pilot_environment() { :; }
require_command() { :; }
validate_ssh_config() { :; }
validate_aws_session() { :; }
validate_rds_secret() { :; }
acquire_pilot_lock() { echo lock; }
run_deploygen() { [[ "$DISTRIBUTED_COMPOSE_DIR" == "$REPO_ROOT/.build/aws-compose" ]]; echo generate; }
ensure_rds_is_provisioned() { [[ "$TERRAFORM_BIN" == custom-terraform ]]; echo infrastructure; }
provision_infrastructure() { echo provision; }
prepare_release() { echo prepare; }
initialize_rds_schema() { echo unexpected-schema; return 1; }
run_experiment() { [[ "$EXPERIMENT_CONFIG" == "$REPO_ROOT/experiments/cloud-scale-w1.yaml" ]]; echo experiment; }
main experiments/cloud-scale-w1.yaml $MOCK_MODE
''', MOCK_MODE=mode, TERRAFORM_BIN="custom-terraform", EXPERIMENT_CONFIG="ignored.yaml")
            self.assertEqual(result.returncode, 0, result.stderr)
            actions = [line for line in result.stdout.splitlines() if not line.startswith("[run-full]")]
            expected = (["lock", "infrastructure", "experiment"] if mode == "--reuse-release" else
                        ["lock", "generate", "provision" if mode else "infrastructure", "prepare", "experiment"])
            self.assertEqual(actions, expected)

    def test_schema_runs_after_stop_and_failure_prevents_start(self):
        for sink, schema_exit in (("postgres", "0"), ("postgres", "23"), ("log", "0")):
            result = self.shell('''
source deploy/scripts/aws/run-experiment.sh
for name in require_command validate_inputs acquire_pilot_lock validate_positive_integer \
  load_experiment_description load_terraform_addresses wait_for_all_ssh initialize_artifacts \
  verify_prepared_releases collect_instance_identities check_host_budgets; do
  eval "$name() { :; }"
done
PUBLIC_IPS[cloud-core]=mock-host
jq() { echo "$MOCK_SINK"; }
reset_previous_run() { echo stopped; }
ssh_run() {
  [[ "$2" == 'bash /opt/continuum/current/deploy/scripts/aws/init-rds-schema.sh' ]] || return 99
  echo schema
  return "$MOCK_SCHEMA_EXIT"
}
# Stop before starting any actual collectors/services.
start_metric_collectors() { echo ready-to-start; return 77; }
main_run
''', PYTHON_BIN="true", MOCK_SINK=sink, MOCK_SCHEMA_EXIT=schema_exit)
            self.assertEqual(result.returncode, 23 if schema_exit == "23" else 77, result.stderr)
            actions = [line for line in result.stdout.splitlines() if not line.startswith("[run-experiment]")]
            expected = ["stopped"] + (["schema"] if sink == "postgres" else [])
            if schema_exit == "0":
                expected.append("ready-to-start")
            self.assertEqual(actions, expected)

    def test_lock_rejects_independent_run_and_allows_inherited_child(self):
        available = self.shell('command -v flock')
        if available.returncode:
            self.skipTest("flock requires WSL/Linux")
        with tempfile.TemporaryDirectory() as directory:
            result = self.shell('''
set -eu
source deploy/scripts/aws/pilot-lock.sh
acquire_pilot_lock "$MOCK_ROOT"
bash -c 'source deploy/scripts/aws/pilot-lock.sh; acquire_pilot_lock "$MOCK_ROOT"'
if (unset CONTINUUM_PILOT_LOCK; exec 9>&-; acquire_pilot_lock "$MOCK_ROOT"); then
  echo 'Independent run wrongly acquired the lock' >&2
  exit 1
fi
''', MOCK_ROOT=Path(directory).as_posix())
            self.assertEqual(result.returncode, 0, result.stderr)
