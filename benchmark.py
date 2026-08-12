import os
import time
import requests
import urllib.request
import gzip
import shutil
import json
from pyspark.sql import SparkSession
from graphframes import GraphFrame

DATA_URL = "https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz"
DATA_GZ = "soc-sign-bitcoinotc.csv.gz"
DATA_CSV = "soc-sign-bitcoinotc.csv"

def setup_hadoop_winutils():
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        bin_dir = os.path.join(hadoop_home, 'bin')
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, 'winutils.exe')
        hadoop_dll_path = os.path.join(bin_dir, 'hadoop.dll')
        
        base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/"
        
        if not os.path.exists(winutils_path):
            print("Downloading winutils.exe for Windows...")
            try:
                urllib.request.urlretrieve(base_url + "winutils.exe", winutils_path)
            except Exception as e:
                print(f"Failed to download winutils: {e}")
            
        if not os.path.exists(hadoop_dll_path):
            print("Downloading hadoop.dll for Windows...")
            try:
                urllib.request.urlretrieve(base_url + "hadoop.dll", hadoop_dll_path)
            except Exception as e:
                print(f"Failed to download hadoop.dll: {e}")
            
        os.environ['HADOOP_HOME'] = hadoop_home
        # Also need to add to PATH so it can find hadoop.dll
        os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')
        print(f"Set HADOOP_HOME to {hadoop_home}")

def download_dataset():
    if not os.path.exists(DATA_CSV):
        print("Downloading Bitcoin OTC dataset...")
        urllib.request.urlretrieve(DATA_URL, DATA_GZ)
        print("Extracting dataset...")
        with gzip.open(DATA_GZ, 'rb') as f_in:
            with open(DATA_CSV, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print("Dataset ready.")

def get_spark_metrics(spark, app_id, start_time, end_time):
    ui_url = spark.sparkContext.uiWebUrl
    api_url = f"{ui_url}/api/v1/applications/{app_id}/stages"
    
    try:
        response = requests.get(api_url)
        stages = response.json()
    except Exception as e:
        print(f"Failed to fetch metrics from Spark UI: {e}")
        return {}

    # Aggregate metrics across all stages completed in our time window (rough approximation)
    total_task_time = 0
    shuffle_read_bytes = 0
    shuffle_write_bytes = 0
    memory_bytes_spilled = 0
    disk_bytes_spilled = 0
    num_tasks = 0
    max_task_time = 0
    task_times = []

    for stage in stages:
        if stage.get("status") != "COMPLETE":
            continue
            
        total_task_time += stage.get("executorRunTime", 0)
        shuffle_read_bytes += stage.get("shuffleReadBytes", 0)
        shuffle_write_bytes += stage.get("shuffleWriteBytes", 0)
        memory_bytes_spilled += stage.get("memoryBytesSpilled", 0)
        disk_bytes_spilled += stage.get("diskBytesSpilled", 0)
        num_tasks += stage.get("numCompleteTasks", 0)
        
        # To get detailed task metrics (like skew), we'd need to fetch /stages/{stageId}/{stageAttemptId}/taskList
        # For now, we use the stage-level aggregates
        try:
            stage_id = stage["stageId"]
            attempt_id = stage["attemptId"]
            task_api_url = f"{api_url}/{stage_id}/{attempt_id}/taskList?length=10000"
            task_resp = requests.get(task_api_url)
            tasks = task_resp.json()
            for task in tasks:
                t_time = task.get("duration", 0)
                task_times.append(t_time)
                if t_time > max_task_time:
                    max_task_time = t_time
        except Exception as e:
            pass

    task_times.sort()
    median_task_time = task_times[len(task_times)//2] if task_times else 0
    task_skew_ratio = max_task_time / median_task_time if median_task_time > 0 else 0

    return {
        "runtime_seconds": end_time - start_time,
        "total_executor_run_time_ms": total_task_time,
        "shuffle_read_bytes": shuffle_read_bytes,
        "shuffle_write_bytes": shuffle_write_bytes,
        "memory_bytes_spilled": memory_bytes_spilled,
        "disk_bytes_spilled": disk_bytes_spilled,
        "num_tasks": num_tasks,
        "max_task_time_ms": max_task_time,
        "median_task_time_ms": median_task_time,
        "task_skew_ratio": task_skew_ratio
    }

def main():
    setup_hadoop_winutils()
    download_dataset()

    print("Initializing Spark Session...")
    # Add graphframes package
    # We use a commonly compatible version for PySpark 3.x
    spark = SparkSession.builder \
        .appName("GraphFrames Benchmark") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark.sparkContext.applicationId

    print("Loading data into GraphFrame...")
    # Data format: SOURCE, TARGET, RATING, TIME
    edges_df = spark.read.csv(DATA_CSV) \
        .toDF("src", "dst", "rating", "time")
        
    # We only need the graph structure for motif finding
    edges_df = edges_df.select("src", "dst").cache()
    
    # Extract unique vertices
    vertices_df = edges_df.select("src").union(edges_df.select("dst")).distinct() \
        .withColumnRenamed("src", "id").cache()
        
    g = GraphFrame(vertices_df, edges_df)
    
    print(f"Graph loaded: {g.vertices.count()} vertices, {g.edges.count()} edges")

    print("\nRunning Motif Query Baseline...")
    # Example motif: finding a triangle
    # A -> B, B -> C, C -> A
    start_time = time.time()
    
    motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
    
    # Force execution
    motif_count = motifs.count()
    
    end_time = time.time()
    print(f"Motif query completed. Found {motif_count} patterns.")

    print("\nCollecting metrics from Spark REST API...")
    metrics = get_spark_metrics(spark, app_id, start_time, end_time)
    
    print("\n--- Benchmark Results ---")
    print(json.dumps(metrics, indent=4))
    
    with open("results.json", "w") as f:
        json.dump(metrics, f, indent=4)
        
    print("\n" + "="*60)
    print("Spark is paused! You can now view the Spark Web UI.")
    print("Open your browser and navigate to: http://localhost:4040")
    print("Go to the 'Storage' tab to see the GraphFrames cached in memory.")
    print("="*60 + "\n")
    
    input("Press Enter here when you are done to stop Spark and exit... ")
        
    spark.stop()

if __name__ == "__main__":
    main()
