#!/usr/bin/env python3
"""
print_results.py
================
Quickly prints the benchmark results stored in pokec_benchmark_results.json
to the terminal without re-running Spark or re-processing the dataset.
"""

import json
import os
import sys

RESULTS_FILE = "pokec_benchmark_results.json"

def main():
    if not os.path.exists(RESULTS_FILE):
        print(f"Error: {RESULTS_FILE} not found in current directory.")
        sys.exit(1)

    with open(RESULTS_FILE, "r") as f:
        data = json.load(f)

    print("=" * 95)
    print("EMPIRICAL BENCHMARK RESULTS: TOPOLOGY-AWARE SKEW HANDLING (soc-pokec)")
    print("=" * 95)

    # 1. Dataset & Topology Info
    topo = data.get("topology", {})
    edges = data.get("edges", 0)
    print(f"\n[DATASET SUMMARY]")
    print(f"  Dataset:             {data.get('dataset', 'soc-pokec')}")
    print(f"  Total Edges:         {edges:,}")
    print(f"  Total Vertices:      {topo.get('vertices', 0):,}")
    print(f"  Max In-degree:       {topo.get('max_in_deg', 0):,}")
    print(f"  Max Out-degree:      {topo.get('max_out_deg', 0):,}")
    print(f"  Max Hub Work (d-*d+):{topo.get('max_work', 0):,} paths (Vertex {topo.get('top_hubs', [{}])[0].get('id', 'N/A')})")
    print(f"  Total Hubs (>1M w):  {topo.get('num_hubs', 0)}")

    # 2. Main Comparison Summary Table
    print("\n" + "=" * 95)
    print(f"{'Condition':<22} | {'Wall (s)':<14} | {'Max Task':<14} | {'Median Task':<14} | {'CV':<8} | {'Mem Spill':<11} | {'Disk Spill':<10}")
    print("-" * 95)

    results = data.get("results", {})
    for cond_key, cond_val in results.items():
        name = cond_val.get("condition", cond_key)
        summary = cond_val.get("summary", {})

        def get_stat(key, factor=1.0, unit=""):
            item = summary.get(key)
            if item is None:
                return "N/A"
            mean = item.get("mean", 0) * factor
            std = item.get("std", 0) * factor
            if unit:
                return f"{mean:.1f}{unit} +/- {std:.1f}"
            return f"{mean:.2f} +/- {std:.2f}"

        wall = get_stat("wall_clock_s", unit="s")
        max_task = get_stat("max_task_ms", factor=0.001, unit="s")
        med_task = get_stat("median_task_ms", unit="ms")
        cv = f"{summary.get('task_cv', {}).get('mean', 0):.3f}"

        # Memory spill (from raw runs if summary not directly calculating MB)
        runs = cond_val.get("runs", [])
        mem_spills = [r.get("mem_spill_bytes", 0) for r in runs]
        disk_spills = [r.get("disk_spill_bytes", 0) for r in runs]
        avg_mem_mb = (sum(mem_spills) / len(mem_spills) / (1024 * 1024)) if runs else 0
        avg_disk_mb = (sum(disk_spills) / len(disk_spills) / (1024 * 1024)) if runs else 0

        mem_str = f"{avg_mem_mb:.1f} MB" if avg_mem_mb > 0 else "0 MB"
        disk_str = f"{avg_disk_mb:.1f} MB" if avg_disk_mb > 0 else "0 MB"

        print(f"{name:<22} | {wall:<14} | {max_task:<14} | {med_task:<14} | {cv:<8} | {mem_str:<11} | {disk_str:<10}")

    print("=" * 95)

    # 3. Individual Run Breakdown
    print("\n[DETAILED RUNS BREAKDOWN]")
    for cond_key, cond_val in results.items():
        name = cond_val.get("condition", cond_key)
        runs = cond_val.get("runs", [])
        print(f"\n  -- {name} ({len(runs)} measured runs) --")
        for idx, r in enumerate(runs, 1):
            tasks = r.get("num_tasks", 0)
            wall = r.get("wall_clock_s", 0)
            maxt = r.get("max_task_ms", 0)
            medt = r.get("median_task_ms", 0)
            cv = r.get("task_cv", 0)
            mem_mb = r.get("mem_spill_bytes", 0) / (1024 * 1024)
            disk_mb = r.get("disk_spill_bytes", 0) / (1024 * 1024)
            paths = r.get("path_count", 0)
            print(f"    Run {idx}: {wall:.1f}s | Tasks: {tasks} | Max: {maxt}ms | Median: {medt}ms | CV: {cv:.3f} | Mem Spill: {mem_mb:.1f}MB | Disk Spill: {disk_mb:.1f}MB | Paths: {paths:,}")

    print("\n" + "=" * 95)

if __name__ == "__main__":
    main()
