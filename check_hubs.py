import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame

def main():
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            bin_dir = os.path.join(hadoop_home, 'bin')
            os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')

    spark = SparkSession.builder \
        .appName("HubStats") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    edges_df = spark.read.csv("soc-sign-bitcoinotc.csv").toDF("src", "dst", "rating", "time")
    vertices_src = edges_df.select("src").withColumnRenamed("src", "id")
    vertices_dst = edges_df.select("dst").withColumnRenamed("dst", "id")
    vertices_df = vertices_src.union(vertices_dst).distinct()

    g = GraphFrame(vertices_df, edges_df)
    
    degrees = g.degrees
    in_degrees = g.inDegrees
    out_degrees = g.outDegrees
    
    # Get top 6 hubs (> 500 degree)
    top_hubs = degrees.filter("degree > 500").join(in_degrees, "id", "left").join(out_degrees, "id", "left")
    top_hubs = top_hubs.fillna(0, subset=["inDegree", "outDegree"])
    print("\n--- Top 6 Extreme Hubs ---")
    top_hubs.orderBy(col("degree").desc()).show()
    
    # Calculate replication volume for N = ceil(degree / 50)
    # Replicating e1 (incoming edges) N times.
    # Total replicated rows added = sum(inDegree * N) - sum(inDegree)
    hubs_all = degrees.filter("degree > 50")
    hubs_stats = hubs_all.join(in_degrees, "id", "left").fillna(0, subset=["inDegree"])
    
    rows = hubs_stats.collect()
    total_original_edges = edges_df.count()
    total_extra_rows = 0
    
    for row in rows:
        n = (row['degree'] + 49) // 50 # ceil(degree / 50)
        extra_rows = row['inDegree'] * (n - 1)
        total_extra_rows += extra_rows
        
    print(f"Total Original Edges: {total_original_edges}")
    print(f"Total Extra Rows from Replication: {total_extra_rows}")
    print(f"Percentage Increase: {(total_extra_rows / total_original_edges) * 100:.2f}%")
    
    spark.stop()

if __name__ == "__main__":
    main()
