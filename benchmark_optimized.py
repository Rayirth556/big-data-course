import os
import time
import requests
import statistics
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode
from graphframes import GraphFrame

def get_spark_metrics(app_id):
    """Fetch metrics from the Spark REST API for the most recent stage."""
    time.sleep(2) # Give Spark UI a moment to update
    try:
        stages_url = f"http://localhost:4040/api/v1/applications/{app_id}/stages"
        stages = requests.get(stages_url).json()
        
        # Filter for completed stages that actually processed data
        completed_stages = [s for s in stages if s['status'] == 'COMPLETE' and s.get('numTasks', 0) > 0]
        
        # Get the stage with the longest task time (likely the join stage)
        target_stage = max(completed_stages, key=lambda s: s.get('executorRunTime', 0))
        
        stage_id = target_stage['stageId']
        stage_attempt = target_stage['attemptId']
        
        tasks_url = f"http://localhost:4040/api/v1/applications/{app_id}/stages/{stage_id}/{stage_attempt}/taskList?length=10000"
        tasks_response = requests.get(tasks_url).json()
        tasks = tasks_response if isinstance(tasks_response, list) else []

        if not tasks:
            return {"error": "No tasks found for the stage."}

        # Calculate task runtimes
        task_times = sorted([t.get('duration', 0) for t in tasks])
        
        if not task_times:
             return {"error": "No valid task durations found."}
             
        max_time = task_times[-1]
        median_time = task_times[len(task_times) // 2]
        
        skew_ratio = max_time / median_time if median_time > 0 else max_time
        
        total_shuffle_read = sum(s.get('shuffleReadBytes', 0) for s in completed_stages)
        total_shuffle_write = sum(s.get('shuffleWriteBytes', 0) for s in completed_stages)
        
        return {
            "runtime_seconds": sum(s.get('executorRunTime', 0) for s in completed_stages) / 1000.0,
            "shuffle_read_bytes": total_shuffle_read,
            "shuffle_write_bytes": total_shuffle_write,
            "num_tasks": target_stage.get('numTasks', 0),
            "max_task_time_ms": max_time,
            "median_task_time_ms": median_time,
            "task_skew_ratio": skew_ratio
        }
    except Exception as e:
        return {"error": str(e)}

def setup_env():
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            bin_dir = os.path.join(hadoop_home, 'bin')
            os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')

def run_condition(name, use_aqe=False, use_salting=False):
    print(f"\n{'='*50}")
    print(f"Running {name}")
    print(f"{'='*50}")
    
    spark = SparkSession.builder \
        .appName(f"Benchmark_{name.replace(' ', '')}") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
        .config("spark.sql.adaptive.enabled", "true" if use_aqe else "false") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark.sparkContext.applicationId
    
    edges = spark.read.csv("soc-sign-bitcoinotc.csv").toDF("src", "dst", "rating", "time")
    
    start_time = time.time()
    
    if use_salting:
        t0 = time.time()
        # Build degrees to find hubs
        vertices_src = edges.select("src").withColumnRenamed("src", "id")
        vertices_dst = edges.select("dst").withColumnRenamed("dst", "id")
        vertices = vertices_src.union(vertices_dst).distinct()
        
        g = GraphFrame(vertices, edges)
        degrees = g.degrees
        # Force materialization for timing
        degrees.cache().count()
        t_degrees = time.time()
        
        # Calculate N = ceil(degree / 50) for hubs (> 50)
        hub_stats = degrees.filter("degree > 50").withColumn("N", expr("CAST(ceil(degree / 50.0) AS INT)")).cache()
        
        # e1: Incoming edges to the hub (replicate N times)
        e1_base = edges.alias("e1")
        e1_with_hubs = e1_base.join(hub_stats, col("e1.dst") == col("id"), "left").fillna(1, subset=["N"])
        
        e1_salted = e1_with_hubs.withColumn(
            "salt_array", expr("sequence(0, N - 1)")
        ).withColumn("salt", explode("salt_array")).drop("salt_array", "degree", "N", "id").alias("e1")
        
        # e2: Outgoing edges from the hub (assign single reproducible salt)
        e2_base = edges.alias("e2")
        e2_with_hubs = e2_base.join(hub_stats, col("e2.src") == col("id"), "left").fillna(1, subset=["N"])
        
        e2_salted = e2_with_hubs.withColumn(
            "salt", expr("abs(hash(src, dst, time)) % N")
        ).drop("degree", "N", "id").alias("e2")
        
        # Force materialization
        e1_salted.cache().count()
        e2_salted.cache().count()
        t_replicate = time.time()
        
        # Join
        result = e1_salted.join(e2_salted, (col("e1.dst") == col("e2.src")) & (col("e1.salt") == col("e2.salt")))
        count = result.count()
        t_join = time.time()
        
        times = {
            "time_degrees": t_degrees - t0,
            "time_replicate": t_replicate - t_degrees,
            "time_join": t_join - t_replicate
        }
    else:
        e1 = edges.alias("e1")
        e2 = edges.alias("e2")
        result = e1.join(e2, col("e1.dst") == col("e2.src"))
        count = result.count()
        end_time = time.time()
        times = {
            "time_degrees": 0.0,
            "time_replicate": 0.0,
            "time_join": end_time - start_time
        }
        
    print(f"2-Hop Paths Found: {count}")
    print(f"Phase Times: {times}")
    
    metrics = get_spark_metrics(app_id)
    metrics.update(times)
    metrics['total_wall_clock'] = sum(times.values())
    
    print("\n--- Stage Metrics ---")
    print(json.dumps(metrics, indent=4))
    
    spark.stop()
    return metrics

def run_iterations(name, use_aqe=False, use_salting=False, iters=6):
    results = []
    # Iteration 0 is warmup, 1-5 are measured
    for i in range(iters):
        label = "WARMUP" if i == 0 else f"Run {i}"
        print(f"\nIteration {i}/{iters-1} ({label})")
        metrics = run_condition(name, use_aqe, use_salting)
        if i > 0:
            results.append(metrics)
        
    print(f"\n{'-'*30}\nAggregated Results for {name} (N={iters-1}, excluding warmup):")
    keys_to_agg = ['total_wall_clock', 'task_skew_ratio', 'max_task_time_ms', 'shuffle_read_bytes', 'time_degrees', 'time_replicate', 'time_join']
    for k in keys_to_agg:
        vals = [r.get(k, 0) for r in results if k in r]
        if vals:
            mean_val = statistics.mean(vals)
            std_val = statistics.stdev(vals) if len(vals) > 1 else 0.0
            raw_vals_str = ", ".join([f"{v:.2f}" for v in vals])
            print(f"{k}: {mean_val:.2f} ± {std_val:.2f}  [Raw: {raw_vals_str}]")
    return results

def main():
    setup_env()
    
    print("Starting Phase 2 Benchmarks (N=5, +1 Warmup)...")
    
    # Run the 3 conditions
    run_iterations("Condition 1: Baseline (AQE Off)", use_aqe=False, use_salting=False)
    run_iterations("Condition 2: Baseline (AQE On)", use_aqe=True, use_salting=False)
    run_iterations("Condition 3: Manual Salted Join (AQE Off)", use_aqe=False, use_salting=True)
    
    print("\n\n" + "="*50)
    print("BENCHMARK SUITE COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()
