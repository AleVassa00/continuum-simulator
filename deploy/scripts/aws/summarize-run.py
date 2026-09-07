#!/usr/bin/env python3
"""Export AWS run artifacts using only the Python standard library.

Exit 0: complete measurements and loss-free run; 2: measured but fails quality
checks; 1: missing/malformed measurements. Never turn missing data into zeros.
"""
import argparse
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import re
import statistics


GROUPS = {"cloud-workers": ("edge-aggregates", 6),
          "global-aggregator": ("cloud-edge-aggregates", 1)}
UNITS = {"B": 1, "kB": 1000, "MB": 1000**2, "GB": 1000**3,
         "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}


def timestamp(value):
    # Docker/date emit nanoseconds; Python 3.9 accepts microseconds.
    value = re.sub(r"(\.\d{6})\d+", r"\1", value)
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError(f"Timestamp has no timezone: {value}")
    return result


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_csv(path, rows, fields=None):
    if not rows and fields is None:
        raise ValueError(f"No rows for {path.name}")
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def records(text, marker):
    result = []
    for line in text.splitlines():
        if marker + " " in line:
            result.append(json.loads(line.split(marker + " ", 1)[1]))
    return result


def unique_sites(rows, expected, label):
    ids = [row["edge_id"] for row in rows]
    if len(ids) != len(set(ids)) or set(ids) != set(expected):
        raise ValueError(f"{label}: expected one row per site {sorted(expected)}, got {ids}")


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def number_unit(value):
    match = re.fullmatch(r"([0-9.]+)\s*([A-Za-z]+)", value.strip())
    if not match or match[2] not in UNITS:
        raise ValueError(f"Unknown Docker memory quantity: {value}")
    result = float(match[1]) * UNITS[match[2]]
    if not math.isfinite(result):
        raise ValueError(f"Non-finite Docker memory quantity: {value}")
    return result


def parse_lag(text):
    """Only accept complete, successful partition snapshots, including final ones."""
    result, current, partitions = [], None, []
    for line in text.splitlines():
        match = re.fullmatch(r"===== group (\S+) sample (\S+) =====", line)
        if match:
            current = (match[1], match[2])
            partitions = []
        elif line.startswith("===== query_exit ") and current:
            group, observed = current
            expected_topic, expected_count = GROUPS[group]
            completion = re.fullmatch(r"===== query_exit 0(?: at (\S+))? =====", line)
            if completion and completion[1]:
                observed = completion[1]
            complete = (completion is not None and
                        len(partitions) == expected_count and
                        {row["partition"] for row in partitions} == set(range(expected_count)) and
                        all(row["topic"] == expected_topic for row in partitions))
            if complete:
                result.extend({"timestamp": observed, "group": group, **row} for row in partitions)
            current, partitions = None, []
        elif current:
            fields = line.split()
            if len(fields) >= 6 and fields[0] == current[0]:
                if all(value.isdigit() for value in fields[2:6]):
                    partitions.append(dict(zip(
                        ("topic", "partition", "current_offset", "log_end_offset", "lag"),
                        (fields[1], *map(int, fields[2:6])))))
    return result


def lag_totals(rows, group):
    samples = {}
    for row in rows:
        if row["group"] == group:
            samples[row["timestamp"]] = samples.get(row["timestamp"], 0) + row["lag"]
    return samples


def container_samples(directory, start, end):
    rows = []
    for role in ("simulator", "edge", "cloud-core", "workers"):
        observed = None
        in_stats = False
        text = (directory / "metrics" / f"{role}.log").read_text(encoding="utf-8-sig")
        for line in text.splitlines():
            match = re.fullmatch(r"===== sample (\S+) =====", line)
            if match:
                observed = match[1]
                in_stats = False
            elif line.startswith("-- "):
                in_stats = line == "-- docker-stats"
            elif in_stats and line.startswith("{") and observed:
                if not start <= timestamp(observed) <= end:
                    continue
                stats = json.loads(line)
                usage, limit = stats["MemUsage"].split("/")
                cpu = float(stats["CPUPerc"].rstrip("%"))
                if not math.isfinite(cpu) or cpu < 0:
                    raise ValueError("Docker CPU percentage must be finite and nonnegative")
                rows.append({"timestamp": observed, "role": role, "container": stats["Name"],
                             "cpu_pct": cpu,
                             "memory_bytes": number_unit(usage),
                             "memory_limit_bytes": number_unit(limit)})
    return rows


def summarize_containers(rows):
    groups = {}
    for row in rows:
        groups.setdefault((row["role"], row["container"]), []).append(row)
    result = []
    for (role, container), samples in sorted(groups.items()):
        cpu, memory = ([row[field] for row in samples] for field in ("cpu_pct", "memory_bytes"))
        result.append({"role": role, "container": container, "samples": len(samples),
                       "cpu_avg_pct": statistics.mean(cpu), "cpu_max_pct": max(cpu),
                       "memory_avg_mib": statistics.mean(memory) / 1024**2,
                       "memory_max_mib": max(memory) / 1024**2})
    return result


def summarize(directory):
    metadata = read_json(directory / "run-metadata.json")
    config = read_json(directory / "compose" / "simulator.normalized.json")
    environments = [service["environment"] for service in config["services"].values()]
    expected = [env["SITE_ID"] for env in environments]
    epochs = {env["REPLAY_EPOCH"] for env in environments}
    factors = {float(env["ACCELERATION_FACTOR"]) for env in environments}
    if len(expected) != len(set(expected)) or len(epochs) != 1 or len(factors) != 1:
        raise ValueError("Simulator sites or replay configuration are inconsistent")
    epoch, factor = timestamp(epochs.pop()), factors.pop()
    if not math.isfinite(factor) or factor <= 0:
        raise ValueError("Acceleration must be finite and positive")
    start = timestamp(metadata["replay_start_at"])
    logs = {role: (directory / "logs" / f"{role}.log").read_text(encoding="utf-8-sig")
            for role in ("simulator", "edge", "cloud-core")}
    simulators = records(logs["simulator"], "SIMULATOR_STATS")
    edges = records(logs["edge"], "EDGE_STATS")
    globals_ = records(logs["cloud-core"], "GLOBAL_AGGREGATE")
    unique_sites(simulators, expected, "SIMULATOR_STATS")
    unique_sites(edges, expected, "EDGE_STATS")
    if not globals_:
        raise ValueError("No GLOBAL_AGGREGATE output")
    completions = re.findall(r"(?m)^(\S+) GLOBAL_REPLAY_COMPLETED\s*$", logs["cloud-core"])
    if len(completions) != 1:
        raise ValueError("Expected exactly one timestamped GLOBAL_REPLAY_COMPLETED")
    end = timestamp(completions[0])
    if end <= start:
        raise ValueError("Replay completion must follow REPLAY_START_AT")

    failures = []
    if metadata.get("status") not in ("completed", "success") or metadata.get("orchestrator_exit_code") != 0:
        failures.append("orchestration_failed")
    summary = {"run_id": metadata["run_id"], "status": metadata["status"],
               "workers": metadata["workers"], "acceleration_factor": factor,
               "replay_elapsed_seconds": (end - start).total_seconds(),
               "measurement_start_at": start.isoformat(), "measurement_end_at": end.isoformat()}
    sim_fields = {"simulator_offered_total": "offered_events",
                  "simulator_enqueued_total": "telemetry_enqueued",
                  "simulator_locally_dropped_total": "telemetry_locally_dropped",
                  "simulator_mqtt_errors_total": "mqtt_publish_errors",
                  "simulator_eos_failures_total": "eos_failures"}
    edge_fields = {"edge_received_total": "telemetry_received", "edge_processed_total": "processed",
                   "edge_ingress_queue_dropped_total": "ingress_queue_dropped",
                   "edge_invalid_total": "invalid_telemetry", "edge_out_of_order_dropped_total": "out_of_order_dropped",
                   "edge_post_eos_dropped_total": "post_eos_dropped",
                   "edge_aggregates_emitted_total": "aggregates_emitted"}
    for target, source in sim_fields.items():
        summary[target] = sum(row[source] for row in simulators)
    for target, source in edge_fields.items():
        summary[target] = sum(row[source] for row in edges)
    summary["simulator_max_scheduling_lag_ms"] = max(row["max_scheduling_lag_ms"] for row in simulators)
    summary["edge_max_queue_utilization_pct"] = max(row["max_ingress_queue_utilization_pct"] for row in edges)
    summary["global_events_total"] = sum(row["events"] for row in globals_)
    summary["offered_minus_global_events"] = summary["simulator_offered_total"] - summary["global_events_total"]
    summary["processed_minus_global_events"] = summary["edge_processed_total"] - summary["global_events_total"]
    summary["global_windows_total"] = len(globals_)
    summary["global_duplicate_ids_total"] = len(globals_) - len({row["aggregate_id"] for row in globals_})
    summary["global_incomplete_windows_total"] = sum(row["contributing_edges"] != len(expected) or
                                                    row["expected_edges"] != len(expected) for row in globals_)
    summary["global_late_aggregates_dropped_total"] = logs["cloud-core"].count("late aggregate scartato")
    for key in ("simulator_locally_dropped_total", "simulator_mqtt_errors_total", "simulator_eos_failures_total",
                "edge_ingress_queue_dropped_total", "edge_invalid_total", "edge_out_of_order_dropped_total",
                "edge_post_eos_dropped_total", "offered_minus_global_events", "processed_minus_global_events",
                "global_duplicate_ids_total", "global_incomplete_windows_total", "global_late_aggregates_dropped_total"):
        if summary[key] != 0:
            failures.append(key)
    if summary["edge_max_queue_utilization_pct"] >= 100:
        failures.append("edge_queue_reached_capacity")
    for sim, edge in ((sim, next(row for row in edges if row["edge_id"] == sim["edge_id"])) for sim in simulators):
        if sim["status"] != "completato" or sim["eos_successes"] != 1 or edge["end_of_replay_processed"] != 1:
            failures.append(f"incomplete_eos:{sim['edge_id']}")
        if sim["mqtt_publish_attempts"] != edge["telemetry_received"]:
            failures.append(f"mqtt_delivery_count:{sim['edge_id']}")

    window_rows, latencies = [], []
    terminal_end = max(timestamp(row["window_end"]) for row in globals_)
    for row in sorted(globals_, key=lambda row: row["window_start"]):
        window_end = timestamp(row["window_end"])
        delay = (timestamp(row["emitted_at"]) - start).total_seconds() - (window_end - epoch).total_seconds() / factor
        terminal = window_end == terminal_end
        # Exclude the last window conservatively: EOS may flush it before its
        # nominal deadline. This is publication delay, not per-record latency.
        if not terminal:
            latencies.append(delay)
        flat = {key: value for key, value in row.items() if not isinstance(value, dict)}
        flat.update(publication_delay_seconds=delay, excluded_terminal_window=terminal)
        for metric in ("temperature", "humidity", "pressure"):
            flat.update({f"{metric}_{key}": value for key, value in row[metric].items()})
        window_rows.append(flat)
    summary["global_publication_delay_samples"] = len(latencies)
    summary["global_publication_delay_p50_seconds"] = percentile(latencies, 0.5)
    summary["global_publication_delay_p95_seconds"] = percentile(latencies, 0.95)
    summary["global_publication_delay_max_seconds"] = max(latencies) if latencies else None
    if not latencies or any(value < -0.001 for value in latencies):
        failures.append("publication_delay_unavailable_or_negative")

    lag = [row for row in parse_lag((directory / "metrics" / "kafka-lag.log").read_text(encoding="utf-8-sig"))
           if start <= timestamp(row["timestamp"]) <= end]
    final_lag = [row for row in parse_lag((directory / "kafka-consumer-groups-final.txt").read_text(encoding="utf-8-sig"))
                 if timestamp(row["timestamp"]) >= end]
    for group, prefix in (("cloud-workers", "cloud_workers"), ("global-aggregator", "global_aggregator")):
        totals, final = lag_totals(lag, group), lag_totals(final_lag, group)
        summary[f"{prefix}_lag_samples"] = len(totals)
        summary[f"{prefix}_max_total_lag"] = max(totals.values()) if totals else None
        summary[f"{prefix}_final_lag"] = final[max(final, key=timestamp)] if final else None
        if not totals or not final:
            failures.append(f"missing_lag_samples:{group}")
        elif summary[f"{prefix}_final_lag"] != 0:
            failures.append(f"nonzero_final_lag:{group}")
    for group, (topic, _) in GROUPS.items():
        rows = [row for row in final_lag if row["group"] == group]
        latest = max((row["timestamp"] for row in rows), key=timestamp, default=None)
        summary[topic.replace("-", "_") + "_topic_records"] = (
            sum(row["log_end_offset"] for row in rows if row["timestamp"] == latest) if latest else None)

    samples = container_samples(directory, start, end)
    container_summary = summarize_containers(samples)
    expected_containers = {f"simulator-{site}" for site in expected} | set(expected) | {f"mqtt-{site}" for site in expected}
    expected_containers |= {f"cloud-worker-{i}" for i in range(int(metadata["workers"]))} | {"kafka", "global-aggregator"}
    missing = expected_containers - {row["container"] for row in samples}
    if missing:
        failures.append("missing_resource_samples:" + ",".join(sorted(missing)))
    for label, predicate in (("cloud_workers", lambda name: name.startswith("cloud-worker-")),
                             ("kafka", lambda name: name == "kafka")):
        by_sample = {}
        for row in samples:
            if predicate(row["container"]):
                by_sample.setdefault(row["timestamp"], []).append(row)
        required = int(metadata["workers"]) if label == "cloud_workers" else 1
        complete = [rows for rows in by_sample.values() if len(rows) == required]
        cpu = [sum(row["cpu_pct"] for row in rows) for rows in complete]
        memory = [sum(row["memory_bytes"] for row in rows) / 1024**2 for rows in complete]
        summary[f"{label}_resource_samples"] = len(complete)
        for suffix, values in (("cpu_pct", cpu), ("memory_mib", memory)):
            summary[f"{label}_avg_{suffix}"] = statistics.mean(values) if values else None
            summary[f"{label}_max_{suffix}"] = max(values) if values else None
        if not complete:
            failures.append(f"missing_complete_resource_samples:{label}")

    summary["quality_status"] = "pass" if not failures else "fail"
    summary["quality_failures"] = ";".join(failures)
    write_csv(directory / "simulator-stats.csv", simulators)
    write_csv(directory / "edge-stats.csv", edges)
    write_csv(directory / "global-windows.csv", window_rows)
    write_csv(directory / "container-stats.csv", container_summary,
              ["role", "container", "samples", "cpu_avg_pct", "cpu_max_pct", "memory_avg_mib", "memory_max_mib"])
    write_csv(directory / "container-stats-samples.csv", samples,
              ["timestamp", "role", "container", "cpu_pct", "memory_bytes", "memory_limit_bytes"])
    write_csv(directory / "kafka-lag.csv", lag,
              ["timestamp", "group", "topic", "partition", "current_offset", "log_end_offset", "lag"])
    write_csv(directory / "run-summary.csv", [summary])
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact_directory", type=Path)
    args = parser.parse_args()
    try:
        result = summarize(args.artifact_directory)
    except (ValueError, KeyError, OSError, TypeError) as exc:
        result = {"quality_status": "unavailable", "error": str(exc)}
        print(json.dumps(result))
        # Do not leave a previous passing summary after a failed reprocessing.
        write_csv(args.artifact_directory / "run-summary.csv", [result])
        (args.artifact_directory / "postprocess-result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return 1
    (args.artifact_directory / "postprocess-result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({key: result[key] for key in ("run_id", "quality_status", "quality_failures")}))
    return 0 if result["quality_status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
