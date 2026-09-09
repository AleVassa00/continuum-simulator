"""Offline tests: no Docker daemon, AWS credentials, or Go application changes."""
import importlib.util
import csv
import json
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


artifacts = module("summarize-run")
budget = module("check-resource-budget")


def lag_snapshot(group, observed, lag=0):
    topic, count = artifacts.GROUPS[group]
    rows = [f"{group} {topic} {p} {10-lag} 10 {lag} consumer host client" for p in range(count)]
    return f"===== group {group} sample {observed} =====\n" + "\n".join(rows) + "\n===== query_exit 0 =====\n"


def fixture(root):
    for child in ("logs", "metrics", "compose"):
        (root / child).mkdir()
    metadata = {"run_id": "test-run", "workers": 1, "status": "completed", "orchestrator_exit_code": 0,
                "replay_start_at": "2026-09-07T00:00:00Z"}
    (root / "run-metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    config = {"services": {f"simulator-edge-{i}": {"environment": {
        "SITE_ID": f"edge-{i}", "REPLAY_EPOCH": "2025-01-01T00:00:00Z", "ACCELERATION_FACTOR": "10000"}}
        for i in range(2)}}
    (root / "compose/simulator.normalized.json").write_text(json.dumps(config), encoding="utf-8")
    simulators = [{"edge_id": f"edge-{i}", "status": "completato", "offered_events": 2, "telemetry_enqueued": 2,
                   "telemetry_locally_dropped": 0, "mqtt_publish_attempts": 2, "mqtt_publish_errors": 0,
                   "eos_failures": 0, "eos_successes": 1, "max_scheduling_lag_ms": 0} for i in range(2)]
    edges = [{"edge_id": f"edge-{i}", "telemetry_received": 2, "processed": 2, "ingress_queue_dropped": 0,
              "invalid_telemetry": 0, "out_of_order_dropped": 0, "post_eos_dropped": 0, "aggregates_emitted": 2,
              "end_of_replay_processed": 1, "max_ingress_queue_utilization_pct": 20} for i in range(2)]
    for role, marker, rows in (("simulator", "SIMULATOR_STATS", simulators), ("edge", "EDGE_STATS", edges)):
        (root / "logs" / f"{role}.log").write_text("\n".join(marker + " " + json.dumps(row) for row in rows), encoding="utf-8")
    metric = {"valid": 2, "invalid": 0, "sum": 10, "average": 5, "min": 5, "max": 5}
    windows = [{"aggregate_id": f"global-{i}", "window_start": f"2025-01-01T00:{i*15:02}:00Z",
                "window_end": f"2025-01-01T00:{(i+1)*15:02}:00Z", "emitted_at": "2026-09-07T00:00:01Z",
                "expected_partitions": 6, "contributing_partitions": 6, "events": 2,
                "temperature": metric, "humidity": metric, "pressure": metric} for i in range(2)]
    (root / "logs/cloud-core.log").write_text("\n".join("GLOBAL_AGGREGATE " + json.dumps(row) for row in windows) +
                                            "\n2026-09-07T00:00:02.000000001Z GLOBAL_REPLAY_COMPLETED\n", encoding="utf-8")
    for role, names in {"simulator": ["simulator-edge-0", "simulator-edge-1"],
                        "edge": ["edge-0", "edge-1", "mqtt-edge-0", "mqtt-edge-1"],
                        "cloud-core": ["kafka", "global-aggregator"], "workers": ["cloud-worker-0"]}.items():
        (root / "metrics" / f"{role}.log").write_text("===== sample 2026-09-07T00:00:01Z =====\n-- docker-stats\n" +
            "\n".join(json.dumps({"Name": name, "CPUPerc": "25.0%", "MemUsage": "16MiB / 128MiB"}) for name in names), encoding="utf-8")
    (root / "metrics/kafka-lag.log").write_text("".join(lag_snapshot(group, "2026-09-07T00:00:01Z", 2) for group in artifacts.GROUPS), encoding="utf-8")
    (root / "kafka-consumer-groups-final.txt").write_text("".join(lag_snapshot(group, "2026-09-07T00:00:03Z") for group in artifacts.GROUPS), encoding="utf-8")


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        fixture(self.root)

    def test_clean_run_preserves_window_timestamps_without_derived_delay(self):
        log_path = self.root / "logs/cloud-core.log"
        original_log = log_path.read_bytes()
        original_windows = artifacts.records(log_path.read_text(), "GLOBAL_AGGREGATE")
        result = artifacts.summarize(self.root)
        self.assertEqual(result["quality_status"], "pass")
        self.assertEqual(result["cloud_workers_max_total_lag"], 12)
        self.assertEqual(result["cloud_workers_final_lag"], 0)
        self.assertEqual(result["replay_elapsed_seconds"], 2)
        self.assertFalse(any("publication_delay" in key for key in result))
        with (self.root / "global-windows.csv").open(newline="", encoding="utf-8") as stream:
            windows = list(csv.DictReader(stream))
        self.assertEqual(len(windows), len(original_windows))
        for exported, original in zip(windows, original_windows):
            for field in ("window_start", "window_end", "emitted_at"):
                self.assertEqual(exported[field], original[field])
            self.assertNotIn("publication_delay_seconds", exported)
            self.assertNotIn("excluded_terminal_window", exported)
        self.assertEqual(log_path.read_bytes(), original_log)
        self.assertNotIn("publication_delay", (self.root / "run-summary.csv").read_text())

    def test_single_window_no_longer_fails_for_missing_delay_samples(self):
        path = self.root / "logs/cloud-core.log"
        lines = path.read_text().splitlines()
        window = artifacts.records(lines[0], "GLOBAL_AGGREGATE")[0]
        window["events"] = 4
        path.write_text("GLOBAL_AGGREGATE " + json.dumps(window) + "\n" + lines[-1] + "\n", encoding="utf-8")
        self.assertEqual(artifacts.summarize(self.root)["quality_status"], "pass")

    def test_window_emission_no_longer_triggers_negative_delay_check(self):
        path = self.root / "logs/cloud-core.log"
        path.write_text(path.read_text().replace("2026-09-07T00:00:01Z", "2026-09-07T00:00:00Z"), encoding="utf-8")
        result = artifacts.summarize(self.root)
        self.assertEqual(result["quality_status"], "pass")
        self.assertNotIn("publication_delay", result["quality_failures"])

    def test_missing_simulator_is_not_zero_drop_success(self):
        (self.root / "logs/simulator.log").write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "SIMULATOR_STATS"):
            artifacts.summarize(self.root)

    def test_successful_orchestration_with_loss_fails_quality(self):
        path = self.root / "logs/cloud-core.log"
        path.write_text(path.read_text(encoding="utf-8").replace('"events": 2', '"events": 1'), encoding="utf-8")
        result = artifacts.summarize(self.root)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["quality_status"], "fail")
        self.assertEqual(result["offered_minus_global_events"], 2)

    def test_duplicate_stats_rejected(self):
        path = self.root / "logs/edge.log"
        path.write_text(path.read_text() + "\n" + path.read_text().splitlines()[0], encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EDGE_STATS"):
            artifacts.summarize(self.root)

    def test_incomplete_final_partition_snapshot_is_unknown(self):
        path = self.root / "kafka-consumer-groups-final.txt"
        path.write_text("\n".join(line for line in path.read_text().splitlines() if not line.startswith("cloud-workers edge-aggregates 5 ")), encoding="utf-8")
        result = artifacts.summarize(self.root)
        self.assertIsNone(result["cloud_workers_final_lag"])
        self.assertEqual(result["quality_status"], "fail")

    def test_failed_query_with_plausible_rows_is_ignored(self):
        raw = lag_snapshot("cloud-workers", "2026-09-07T00:00:01Z").replace("query_exit 0", "query_exit 1")
        self.assertEqual(artifacts.parse_lag(raw), [])

    def test_empty_periodic_lag_is_not_zero(self):
        (self.root / "metrics/kafka-lag.log").write_text("", encoding="utf-8")
        result = artifacts.summarize(self.root)
        self.assertIsNone(result["cloud_workers_max_total_lag"])
        self.assertEqual(result["quality_status"], "fail")

    def test_final_snapshot_before_completion_is_unknown(self):
        path = self.root / "kafka-consumer-groups-final.txt"
        path.write_text(path.read_text().replace("00:00:03Z", "00:00:01Z"), encoding="utf-8")
        result = artifacts.summarize(self.root)
        self.assertIsNone(result["cloud_workers_final_lag"])
        self.assertEqual(result["quality_status"], "fail")

    def test_query_completion_timestamp_defines_observation(self):
        raw = lag_snapshot("cloud-workers", "2026-09-07T00:00:01Z").replace(
            "query_exit 0 =====", "query_exit 0 at 2026-09-07T00:00:04.123456789Z =====")
        rows = artifacts.parse_lag(raw)
        self.assertEqual(len(rows), 6)
        self.assertEqual(artifacts.timestamp(rows[0]["timestamp"]).microsecond, 123456)
        (self.root / "metrics/kafka-lag.log").write_text(raw, encoding="utf-8")
        self.assertIsNone(artifacts.summarize(self.root)["cloud_workers_max_total_lag"])

    def test_nonfinite_cpu_rejected(self):
        path = self.root / "metrics/workers.log"
        path.write_text(path.read_text().replace("25.0%", "NaN%"), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "CPU percentage"):
            artifacts.summarize(self.root)

    def test_failed_reprocessing_invalidates_old_summary(self):
        artifacts.summarize(self.root)
        (self.root / "logs/simulator.log").write_text("", encoding="utf-8")
        process = subprocess.run([sys.executable, str(Path(artifacts.__file__)), str(self.root)],
                                 capture_output=True, text=True)
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertIn("unavailable", (self.root / "run-summary.csv").read_text())
        self.assertEqual(json.loads((self.root / "postprocess-result.json").read_text())["quality_status"], "unavailable")

    def test_cli_quality_failure_exit_code(self):
        path = self.root / "logs/edge.log"
        path.write_text(path.read_text().replace('"ingress_queue_dropped": 0', '"ingress_queue_dropped": 1'), encoding="utf-8")
        process = subprocess.run([sys.executable, str(Path(artifacts.__file__)), str(self.root)],
                                 capture_output=True, text=True)
        self.assertEqual(process.returncode, 2, process.stderr)

    def test_resource_capacity_and_swap_rejected(self):
        capacity = {"cpus": 2, "memory_bytes": 2 * 1024**3}
        service = {"cpus": 0.25, "mem_limit": 2 * 1024**3, "memswap_limit": 2 * 1024**3}
        with self.assertRaisesRegex(ValueError, "Memory ceilings"):
            budget.check({"services": {"worker": service}}, capacity)
        with self.assertRaisesRegex(ValueError, "no swap"):
            budget.check({"services": {"worker": {**service, "memswap_limit": -1}}}, capacity)
        with self.assertRaisesRegex(ValueError, "positive finite"):
            budget.check({"services": {"worker": service}}, {**capacity, "cpus": "nan"})

    def test_limits_and_resource_reserve(self):
        service = {"cpus": "0.25", "mem_limit": 128 * 1024**2, "memswap_limit": 128 * 1024**2}
        capacity = {"cpus": 2, "memory_bytes": 2 * 1024**3}
        config = {"services": {f"worker-{i}": service for i in range(6)}}
        self.assertEqual(budget.check(config, capacity)["total_cpus"], 1.5)
        config["services"]["worker-6"] = {**service, "cpus": 0.5}
        with self.assertRaisesRegex(ValueError, "CPU ceilings"):
            budget.check(config, capacity)

    def test_missing_limit_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit positive"):
            budget.check({"services": {"worker": {}}}, {"cpus": 2, "memory_bytes": 2 * 1024**3})


if __name__ == "__main__":
    unittest.main()
