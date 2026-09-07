#!/usr/bin/env python3
"""Validate explicit Compose ceilings against a host, without starting services."""
import argparse
import json
import math
from pathlib import Path


def check(config, capacity):
    host_cpus = float(capacity["cpus"])
    host_memory = int(capacity["memory_bytes"])
    if not math.isfinite(host_cpus) or host_cpus <= 0 or host_memory <= 0 or not config["services"]:
        raise ValueError("Nonempty services and positive finite host capacity required")
    rows = []
    for name, service in config["services"].items():
        cpu = float(service.get("cpus", 0))
        memory = int(service.get("mem_limit", 0))
        if not math.isfinite(cpu) or cpu <= 0 or memory <= 0:
            raise ValueError(f"{name}: explicit positive CPU and memory limits required")
        if int(service.get("memswap_limit", -1)) != memory:
            raise ValueError(f"{name}: memswap_limit must equal mem_limit (no swap)")
        rows.append({"container": name, "cpus": cpu, "memory_bytes": memory})
    cpus = sum(row["cpus"] for row in rows)
    memory = sum(row["memory_bytes"] for row in rows)
    # Include startup-only services too: this is deliberately conservative.
    if cpus > host_cpus - 0.25 + 1e-9:
        raise ValueError(f"CPU ceilings {cpus:.2f} exceed host budget (reserve 0.25 CPU)")
    if memory > host_memory - 512 * 1024**2:
        raise ValueError("Memory ceilings exceed host budget (reserve 512 MiB)")
    return {"host": capacity, "total_cpus": cpus, "total_memory_bytes": memory, "services": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("compose", type=Path)
    parser.add_argument("capacity", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(check(json.loads(args.compose.read_text(encoding="utf-8-sig")),
                               json.loads(args.capacity.read_text(encoding="utf-8-sig"))), indent=2))
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(1, f"Resource preflight failed: {exc}\n")


if __name__ == "__main__":
    main()
