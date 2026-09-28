import os
import sys
import time
import statistics
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, explode, sequence, rand, floor

P_CELLS = 100
WORK_THRESHOLD = 250_000
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
        SparkSession.builder.appName("SyntheticBenchmark")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def generate_synthetic_graph(spark):
    """
    Generates a synthetic scale-free graph to mimic Cartesian explosion benchmarks.
    - 5 massive heavy-hitter hubs
    - 1,000,000 random uniform edges (noise)
    """
    print("\nGenerating synthetic scale-free graph...")

    # 1. Noise: 1M random edges between 100k vertices
    noise_edges = spark.range(1_000_000).select(
        floor(rand() * 100_000).cast("int").alias("src"),
        floor(rand() * 100_000).cast("int").alias("dst")
    )

    # 2. Hubs: 5 vertices with 5,000 in-edges and 5,000 out-edges each
    # This guarantees 25,000,000 Cartesian paths per hub!
    hub_edges_in = spark.range(25_000).select(
        (col("id") + 200_000).cast("int").alias("src"),
        (col("id") % 5).cast("int").alias("dst") # Hubs are vertices 0, 1, 2, 3, 4
    )

    hub_edges_out = spark.range(25_000).select(
        (col("id") % 5).cast("int").alias("src"),
        (col("id") + 300_000).cast("int").alias("dst")
    )

    edges = noise_edges.union(hub_edges_in).union(hub_edges_out)
    return edges.repartition(200).cache()

def analyze_topology(edges):
    print("Analyzing topology...")
    out_deg = edges.groupBy("src").count().withColumnRenamed("src", "id").withColumnRenamed("count", "out_deg")
    in_deg = edges.groupBy("dst").count().withColumnRenamed("dst", "id").withColumnRenamed("count", "in_deg")

    degrees = out_deg.join(in_deg, "id", "full").fillna(0)
    degrees = degrees.withColumn("work", col("in_deg") * col("out_deg")).cache()

    hubs = degrees.filter(col("work") > WORK_THRESHOLD)

    hub_meta = (
        hubs.withColumn("raw_r", expr(f"sqrt(in_deg * {P_CELLS}.0 / greatest(out_deg, 1))"))
        .withColumn("raw_s", expr(f"sqrt(out_deg * {P_CELLS}.0 / greatest(in_deg, 1))"))
        .withColumn("r", expr("cast(greatest(1, floor(raw_r)) as int)"))
        .withColumn("s", expr("cast(greatest(1, floor(raw_s)) as int)"))
        .withColumn("scale", expr(f"IF(r * s > {P_CELLS}, sqrt({P_CELLS}.0 / (r * s)), 1.0)"))
        .withColumn("r_final", expr("cast(greatest(1, floor(r * scale)) as int)"))
        .withColumn("s_final", expr("cast(greatest(1, floor(s * scale)) as int)"))
        .select("id", "r_final", "s_final")
        .collect()
    )

    hub_count = len(hub_meta)
    print(f"Hubs identified: {hub_count}")
    return hub_meta

def run_baseline(edges):
    e1 = edges.alias("e1")
    e2 = edges.alias("e2")
    return e1.join(e2, col("e1.dst") == col("e2.src")).count()

def run_hypercube_unified(edges, hub_meta):
    """
    Unified Single-Pass Hypercube:
    Uses a Map Literal to attach (r,s) coordinates without any anti-joins or branching.
    Non-hubs simply fall back to r=1, s=1, exploding into exactly 1 row (no overhead).
    """
    if len(hub_meta) == 0:
        # Fallback dummy map if no hubs found
        hub_map_str = "map(cast(-1 as int), struct(1 as r, 1 as s))"
    else:
        map_entries = []
        for h in hub_meta:
            map_entries.append(f"cast({h['id']} as int), struct(cast({h['r_final']} as int) as r, cast({h['s_final']} as int) as s)")
        hub_map_str = f"map({', '.join(map_entries)})"

    # Left side (incoming to intermediate vertex 'dst')
    e1 = (
        edges.withColumn("hub_info", expr(f"element_at({hub_map_str}, dst)"))
        .withColumn("r", expr("coalesce(hub_info.r, 1)"))
        .withColumn("s", expr("coalesce(hub_info.s, 1)"))
        .withColumn("r_coord", expr("abs(hash(src)) % r"))
        .withColumn("s_coord", expr("explode(sequence(0, s - 1))"))
        .select("src", "dst", "r_coord", "s_coord")
        .alias("e1")
    )

    # Right side (outgoing from intermediate vertex 'src')
    e2 = (
        edges.withColumn("hub_info", expr(f"element_at({hub_map_str}, src)"))
        .withColumn("r", expr("coalesce(hub_info.r, 1)"))
        .withColumn("s", expr("coalesce(hub_info.s, 1)"))
        .withColumn("s_coord", expr("abs(hash(dst)) % s"))
        .withColumn("r_coord", expr("explode(sequence(0, r - 1))"))
        .select("src", "dst", "r_coord", "s_coord")
        .alias("e2")
    )

    # Single unified join for both hubs and non-hubs!
    return e1.join(
        e2,
        (col("e1.dst") == col("e2.src")) &
        (col("e1.r_coord") == col("e2.r_coord")) &
        (col("e1.s_coord") == col("e2.s_coord"))
    ).count()

def snapshot_stages(app_id):
    try:
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        stages = requests.get(url, timeout=5).json()
        return {s["stageId"] for s in stages if s.get("status") == "COMPLETE"}
    except Exception:
        return set()

def collect_duration(app_id, before_ids):
    time.sleep(1)
    try:
        url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        stages = requests.get(url, timeout=5).json()
        completed = {s["stageId"]: s for s in stages if s.get("status") == "COMPLETE"}
        new_ids = sorted(set(completed.keys()) - before_ids)
        if not new_ids: return "?"
        durations = []
        for sid in new_ids:
            attempt = completed[sid].get("attemptId", 0)
            tasks_url = f"http://localhost:4040/api/v1/applications/{app_id}/stages/{sid}/{attempt}/taskList?length=10000"
            tasks = requests.get(tasks_url, timeout=5).json()
            if isinstance(tasks, list):
                durations.extend([t.get("duration", 0) for t in tasks])
        if durations:
            return max(durations)
        return "?"
    except Exception:
        return "?"

def main():
    setup_env()
    spark = create_spark()
    app_id = spark.sparkContext.applicationId
    print(f"Spark UI: http://localhost:4040  (App: {app_id})")

    edges = generate_synthetic_graph(spark)
    edges.count() # Force materialization
    print(f"Graph generated with {edges.count():,} edges.")

    hub_meta = analyze_topology(edges)

    def run_cond(name, hypercube=False):
        print(f"\nCondition: {name}")
        for i in range(WARMUP_RUNS + MEASURED_RUNS):
            before = snapshot_stages(app_id)
            t0 = time.time()
            if hypercube:
                cnt = run_hypercube_unified(edges, hub_meta)
            else:
                cnt = run_baseline(edges)
            wall = time.time() - t0
            max_t = collect_duration(app_id, before)

            lbl = "WARMUP" if i < WARMUP_RUNS else f"RUN {i - WARMUP_RUNS + 1}"
            print(f"  [{lbl}] paths={cnt:,} | wall={wall:.1f}s | max_task={max_t}ms")

    # 1. Baseline AQE Off
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    run_cond("Baseline (AQE Off)")

    # 2. Baseline AQE On
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(1 * 1024 * 1024)) # 1MB to force trigger
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
    run_cond("Baseline (AQE On)")

    # 3. Hypercube Unified
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    run_cond("Hypercube Planner (Unified Map Literal)", hypercube=True)

    spark.stop()

if __name__ == "__main__":
    main()
