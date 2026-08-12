import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hash

def main():
    # Setup Hadoop workaround for Windows
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            bin_dir = os.path.join(hadoop_home, 'bin')
            os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')

    spark = SparkSession.builder \
        .appName("Partitioning Test") \
        .master("local[*]") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # Use existing CSV instead of createDataFrame to avoid serialization issues
    edges = spark.read.csv("soc-sign-bitcoinotc.csv").toDF("src", "dst", "rating", "time").select("src", "dst").limit(100)

    # 1. Add custom partition key
    edges = edges.withColumn("partition_key", hash(col("src")) % 10)

    # 2. Repartition using the custom key
    repartitioned_edges = edges.repartition(10, "partition_key")

    # 3. Alias for self-join (simulating motif: (a)-[e1]->(b); (b)-[e2]->(c))
    e1 = repartitioned_edges.alias("e1")
    e2 = repartitioned_edges.alias("e2")

    # 4. Perform the join on the actual motif keys (e1.dst == e2.src)
    motif_join = e1.join(e2, col("e1.dst") == col("e2.src"))

    print("\n--- Physical Plan for Motif Join ---")
    motif_join.explain(True)
    
    spark.stop()

if __name__ == "__main__":
    main()
