#!/usr/bin/env python3
"""
pokec_benchmark.py
==================
Benchmarks topology-aware hypercube partitioning against the standard
GraphFrames motif join on soc-pokec (1.6M vertices, 30.6M directed edges).

Query: 2-hop motif  (a)-[e1]->(b)-[e2]->(c)
       GraphFrames translates this to: e1.join(e2, e1.dst == e2.src)

Conditions:
  1. Baseline AQE Off  - Standard join, no adaptive optimization
  2. Baseline AQE On   - Standard join, AQE skew join enabled
  3. Hypercube Planner  - Degree-balanced 2D grid partitioning (our approach)

Metrics (via Spark REST API):
  - Max / p95 / median task duration
  - Task-duration coefficient of variation (CV)
  - Shuffle read/write bytes
  - Disk spill bytes
  - End-to-end wall-clock time
"""

import os
import sys
import time
import json
import math
import statistics
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, explode, sequence, broadcast, abs as spark_abs
)

POKEC_PARQUET = "pokec_edges_parquet"
P_CELLS = 100
WORK_THRESHOLD = 1_000_000
WARMUP_RUNS = 1
MEASURED_RUNS = 3


def setup_env():
    if os.name == "nt":
        hadoop_home = os.path.join(os.getcwd(), "hadoop")
        if os.path.exists(hadoop_home):
            os.environ["HADOOP_HOME"] = hadoop_home
            bin_dir = os.path.join(hadoop_home, "bin")
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def create_spark():
    spark = (
        SparkSession.builder.appName("PokecBenchmark")
        .master("local[*]")       # uses all available cpu cores on this local machine
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.driver.memory", "6g")   # gives the spark driver 6gb of ram
        .config("spark.sql.shuffle.partitions", "200")     # sets the number of shuffle reducers to 200
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # disables broadcast and enables SortMergeJoin
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# Metrics collection via Spark REST API
# ---------------------------------------------------------------------------

def snapshot_stages(app_id):
    try:
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        stages = requests.get(url, timeout=5).json()
        return {s["stageId"] for s in stages if s.get("status") == "COMPLETE"}
    except Exception:
        return set()


def collect_metrics(app_id, before_ids):
    time.sleep(1.5)
    try:
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        all_stages = requests.get(url, timeout=5).json()
        completed = {
            s["stageId"]: s
            for s in all_stages
            if s.get("status") == "COMPLETE"
        }
        new_ids = sorted(set(completed.keys()) - before_ids)
        if not new_ids:
            return None

        total_shuffle_read = 0
        total_shuffle_write = 0
        total_disk_spill = 0
        total_mem_spill = 0
        all_durations = []

        for sid in new_ids:
            st = completed[sid]
            total_shuffle_read += st.get("shuffleReadBytes", 0)
            total_shuffle_write += st.get("shuffleWriteBytes", 0)
            total_disk_spill += st.get("diskBytesSpilled", 0)
            total_mem_spill += st.get("memoryBytesSpilled", 0)

            attempt = st.get("attemptId", 0)
            tasks_url = (
                f"http://localhost:4040/api/v1/applications/{app_id}"
                f"/stages/{sid}/{attempt}/taskList?length=10000"
            )
            try:
                tasks = requests.get(tasks_url, timeout=10).json()
                if isinstance(tasks, list):
                    for t in tasks:
                        all_durations.append(t.get("duration", 0))
            except Exception:
                pass

        if not all_durations:
            return None

        all_durations.sort()
        n = len(all_durations)
        mean_d = statistics.mean(all_durations)
        std_d = statistics.stdev(all_durations) if n > 1 else 0.0

        return {
            "num_tasks": n,
            "max_task_ms": all_durations[-1],
            "p95_task_ms": all_durations[int(n * 0.95)],
            "median_task_ms": all_durations[n // 2],
            "task_cv": round(std_d / mean_d, 4) if mean_d > 0 else 0,
            "shuffle_read_bytes": total_shuffle_read,
            "shuffle_write_bytes": total_shuffle_write,
            "disk_spill_bytes": total_disk_spill,
            "mem_spill_bytes": total_mem_spill,
        }
    except Exception as e:
        print(f"  [metrics error] {e}")
        return None


# ---------------------------------------------------------------------------
# Topology analysis
# ---------------------------------------------------------------------------

def analyze_topology(spark, edges):
    print("\n" + "=" * 70)
    print("TOPOLOGY ANALYSIS")
    print("=" * 70)

    out_deg = (
        edges.groupBy("src")
        .count()
        .withColumnRenamed("src", "id")
        .withColumnRenamed("count", "out_deg")
    )
    in_deg = (
        edges.groupBy("dst")
        .count()
        .withColumnRenamed("dst", "id")
        .withColumnRenamed("count", "in_deg")
    )
    degrees = out_deg.join(in_deg, "id", "full").fillna(0)
    degrees = degrees.withColumn("work", col("in_deg") * col("out_deg")).cache()   # calculates cartesian work

    row = degrees.selectExpr(
        "count(*) as n_vertices",
        "max(in_deg) as max_in",
        "max(out_deg) as max_out",
        "max(work) as max_work",
        "avg(in_deg) as avg_in",
        "avg(out_deg) as avg_out",
        "percentile_approx(in_deg, 0.99) as p99_in",
        "percentile_approx(out_deg, 0.99) as p99_out",
    ).collect()[0]

    print(f"  Vertices:           {row['n_vertices']:>12,}")
    print(f"  Avg in-degree:      {row['avg_in']:>12.1f}")
    print(f"  Avg out-degree:     {row['avg_out']:>12.1f}")
    print(f"  Max in-degree:      {row['max_in']:>12,}")
    print(f"  Max out-degree:     {row['max_out']:>12,}")
    print(f"  P99 in-degree:      {row['p99_in']:>12,}")
    print(f"  P99 out-degree:     {row['p99_out']:>12,}")
    print(f"  Max work (d-*d+):   {row['max_work']:>12,}")
    print(f"  Work threshold:     {WORK_THRESHOLD:>12,}")

    hubs = degrees.filter(col("work") > WORK_THRESHOLD)  # filters out the hubs

    hub_meta = (
        hubs.withColumn(
            "raw_r", expr(f"sqrt(in_deg * {P_CELLS}.0 / greatest(out_deg, 1))")
        )
        .withColumn(
            "raw_s", expr(f"sqrt(out_deg * {P_CELLS}.0 / greatest(in_deg, 1))")
        )
        .withColumn("r", expr("cast(greatest(1, floor(raw_r)) as int)"))
        .withColumn("s", expr("cast(greatest(1, floor(raw_s)) as int)"))
        .withColumn(
            "scale",
            expr(f"IF(r * s > {P_CELLS}, sqrt({P_CELLS}.0 / (r * s)), 1.0)"),
        )
        .withColumn("r_final", expr("cast(greatest(1, floor(r * scale)) as int)"))
        .withColumn("s_final", expr("cast(greatest(1, floor(s * scale)) as int)"))
        .select("id", "in_deg", "out_deg", "work", "r_final", "s_final")
        .cache()
    )

    hub_count = hub_meta.count()
    print(f"\n  Hubs identified:    {hub_count:>12,}")

    print("\n  Top 10 hubs by computational work:")
    print(f"  {'Vertex':>10}  {'d-':>7}  {'d+':>7}  {'Work':>14}  {'Grid (r x s)':>12}")
    print("  " + "-" * 60)
    top = hub_meta.orderBy(col("work").desc()).limit(10).collect()
    for h in top:
        print(
            f"  {h['id']:>10}  {h['in_deg']:>7,}  {h['out_deg']:>7,}"
            f"  {h['work']:>14,}  {h['r_final']:>4} x {h['s_final']:<4}"
        )

    hub_ids = hub_meta.select("id").cache()

    topo_info = {
        "vertices": row["n_vertices"],
        "max_in_deg": row["max_in"],
        "max_out_deg": row["max_out"],
        "max_work": row["max_work"],
        "p99_in_deg": row["p99_in"],
        "p99_out_deg": row["p99_out"],
        "num_hubs": hub_count,
        "work_threshold": WORK_THRESHOLD,
        "P_cells": P_CELLS,
        "top_hubs": [
            {
                "id": h["id"],
                "in_deg": h["in_deg"],
                "out_deg": h["out_deg"],
                "work": h["work"],
                "r": h["r_final"],
                "s": h["s_final"],
            }
            for h in top
        ],
    }

    return hub_meta, hub_ids, topo_info


# ---------------------------------------------------------------------------
# Condition runners
# ---------------------------------------------------------------------------

def run_baseline(edges):
    e1 = edges.alias("e1")
    e2 = edges.alias("e2")
    return e1.join(e2, col("e1.dst") == col("e2.src")).count()




def run_hypercube(edges, hub_meta, hub_ids):
    # --- Hub branch: edges through high-work intermediate vertices ---

    # Incoming edges to hubs (e1 side: dst is the hub)
    e1_hub = edges.join(broadcast(hub_meta), edges["dst"] == hub_meta["id"]).select(
        edges["src"],
        edges["dst"],
        hub_meta["r_final"].alias("r"),
        hub_meta["s_final"].alias("s"),
    )
    # Hash src into r row groups, replicate across s columns
    e1_salted = (
        e1_hub.withColumn("r_coord", expr("abs(hash(src)) % r"))
        .withColumn("s_coord", explode(sequence(lit(0), col("s") - 1)))
        .select("src", "dst", "r_coord", "s_coord")
        .alias("e1h")
    )

    # Outgoing edges from hubs (e2 side: src is the hub)
    e2_hub = edges.join(broadcast(hub_meta), edges["src"] == hub_meta["id"]).select(
        edges["src"],
        edges["dst"],
        hub_meta["r_final"].alias("r"),
        hub_meta["s_final"].alias("s"),
    )
    # Hash dst into s column groups, replicate across r rows
    e2_salted = (
        e2_hub.withColumn("s_coord", expr("abs(hash(dst)) % s"))
        .withColumn("r_coord", explode(sequence(lit(0), col("r") - 1)))
        .select("src", "dst", "r_coord", "s_coord")
        .alias("e2h")
    )

    hub_count = e1_salted.join(
        e2_salted,
        (col("e1h.dst") == col("e2h.src"))
        & (col("e1h.r_coord") == col("e2h.r_coord"))
        & (col("e1h.s_coord") == col("e2h.s_coord")),
    ).count()

    # --- Non-hub branch: standard join for remaining edges ---
    e1_nonhub = edges.join(
        broadcast(hub_ids), edges["dst"] == hub_ids["id"], "left_anti"
    ).alias("e1n")
    e2_nonhub = edges.join(
        broadcast(hub_ids), edges["src"] == hub_ids["id"], "left_anti"
    ).alias("e2n")
    nonhub_count = e1_nonhub.join(
        e2_nonhub, col("e1n.dst") == col("e2n.src")
    ).count()

    return hub_count + nonhub_count


# ---------------------------------------------------------------------------
# Benchmark harness
# ---------------------------------------------------------------------------

def run_condition(name, spark, app_id, edges, hub_meta=None, hub_ids=None):
    print(f"\n{'=' * 70}")
    print(f"CONDITION: {name}")
    print(f"{'=' * 70}")

    all_runs = []
    for i in range(WARMUP_RUNS + MEASURED_RUNS):
        label = "WARMUP" if i < WARMUP_RUNS else f"Run {i - WARMUP_RUNS + 1}"
        print(f"\n  [{label}]", end=" ", flush=True)

        before = snapshot_stages(app_id)
        t0 = time.time()

        if hub_meta is not None:
            path_count = run_hypercube(edges, hub_meta, hub_ids)
        else:
            path_count = run_baseline(edges)

        wall_s = time.time() - t0
        metrics = collect_metrics(app_id, before)

        if metrics is None:
            metrics = {}

        metrics["wall_clock_s"] = round(wall_s, 3)
        metrics["path_count"] = path_count

        print(
            f"paths={path_count:,}  wall={wall_s:.1f}s"
            f"  max_task={metrics.get('max_task_ms', '?')}ms"
            f"  cv={metrics.get('task_cv', '?')}"
            f"  spill={metrics.get('disk_spill_bytes', 0) / 1024 / 1024:.1f}MB"
        )

        if i >= WARMUP_RUNS:
            all_runs.append(metrics)

    return all_runs


def aggregate_runs(runs):
    summary = {}
    keys = [
        "wall_clock_s",
        "max_task_ms",
        "p95_task_ms",
        "median_task_ms",
        "task_cv",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "disk_spill_bytes",
    ]
    for k in keys:
        vals = [r[k] for r in runs if k in r and isinstance(r[k], (int, float))]
        if vals:
            summary[k] = {
                "mean": round(statistics.mean(vals), 3),
                "std": round(statistics.stdev(vals), 3) if len(vals) > 1 else 0,
                "raw": vals,
            }
    summary["path_count"] = runs[0].get("path_count", 0) if runs else 0
    return summary


def print_summary(results):
    print("\n\n" + "=" * 90)
    print("BENCHMARK SUMMARY")
    print("=" * 90)

    header = (
        f"{'Condition':<25} | {'Wall (s)':<14} | {'Max Task':<14}"
        f" | {'p95 Task':<14} | {'CV':<10} | {'Spill (MB)':<12}"
    )
    print(header)
    print("-" * 90)

    for name, data in results.items():
        s = data.get("summary", {})

        def fmt(key, unit=""):
            entry = s.get(key)
            if entry is None:
                return "N/A"
            return f"{entry['mean']:.1f}{unit} +/- {entry['std']:.1f}"

        wall = fmt("wall_clock_s", "s")
        maxt = fmt("max_task_ms", "ms")
        p95t = fmt("p95_task_ms", "ms")

        cv_entry = s.get("task_cv")
        cv_str = f"{cv_entry['mean']:.3f}" if cv_entry else "N/A"

        spill_entry = s.get("disk_spill_bytes")
        spill_str = (
            f"{spill_entry['mean'] / 1024 / 1024:.1f}MB" if spill_entry else "N/A"
        )

        print(f"{name:<25} | {wall:<14} | {maxt:<14} | {p95t:<14} | {cv_str:<10} | {spill_str:<12}")

    print("=" * 90)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_env()

    if not os.path.exists(POKEC_PARQUET):
        print(f"Error: {POKEC_PARQUET} not found. Run data preparation first.")
        sys.exit(1)

    spark = create_spark()
    app_id = spark.sparkContext.applicationId
    print(f"Spark UI: http://localhost:4040  (App: {app_id})")

    print("\nLoading Pokec edges...")
    edges = spark.read.parquet(POKEC_PARQUET).select(
        col("src").cast("int"), col("dst").cast("int")
    )
    edges = edges.repartition(200).cache()
    edge_count = edges.count()
    print(f"Edges loaded: {edge_count:,}")

    hub_meta, hub_ids, topo_info = analyze_topology(spark, edges)

    results = {}

    # Condition 1: Baseline AQE Off
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    runs = run_condition("Baseline (AQE Off)", spark, app_id, edges)
    results["baseline_aqe_off"] = {
        "condition": "Baseline (AQE Off)",
        "aqe": False,
        "runs": runs,
        "summary": aggregate_runs(runs),
    }

    # Condition 2: Baseline AQE On
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set(
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
        str(256 * 1024 * 1024),
    )
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    runs = run_condition("Baseline (AQE On)", spark, app_id, edges)
    results["baseline_aqe_on"] = {
        "condition": "Baseline (AQE On)",
        "aqe": True,
        "runs": runs,
        "summary": aggregate_runs(runs),
    }

    # Condition 3: Hypercube Planner
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    runs = run_condition(
        "Hypercube Planner", spark, app_id, edges, hub_meta, hub_ids
    )
    results["hypercube"] = {
        "condition": "Hypercube Planner",
        "aqe": False,
        "runs": runs,
        "summary": aggregate_runs(runs),
    }

    print_summary(results)

    output = {
        "dataset": "soc-pokec",
        "edges": edge_count,
        "topology": topo_info,
        "config": {
            "P_cells": P_CELLS,
            "work_threshold": WORK_THRESHOLD,
            "shuffle_partitions": 200,
            "warmup_runs": WARMUP_RUNS,
            "measured_runs": MEASURED_RUNS,
        },
        "results": results,
    }

    out_path = "pokec_benchmark_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved to {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()
