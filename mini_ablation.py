import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hash, explode, array, expr
from pyspark.sql.types import IntegerType

def main():
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            bin_dir = os.path.join(hadoop_home, 'bin')
            os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')

    spark = SparkSession.builder \
        .appName("MiniAblation") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read from mini.csv to avoid createDataFrame serialization issues
    edges = spark.read.csv("mini.csv").toDF("src", "dst")
    
    # 1. Baseline Join
    e1_base = edges.alias("e1")
    e2_base = edges.alias("e2")
    baseline_join = e1_base.join(e2_base, col("e1.dst") == col("e2.src"))
    
    baseline_count = baseline_join.count()
    print(f"--- Baseline Join ---")
    print(f"Row count: {baseline_count}") # Should be 3 * 2 = 6 paths
    baseline_join.show()

    # 2. Salted Join Logic
    # Hub definition: degree >= 5 (total degree of 2 is 5)
    hub_id = 2
    N = 3 # Let's replicate hub incoming edges 3 times
    
    # Replicate incoming edges to the hub
    # If e1.dst == hub, replicate N times with salt [0, 1, 2]
    # Else salt = 0
    e1_salted = edges.withColumn(
        "salt_array",
        expr(f"IF(dst == {hub_id}, sequence(0, {N-1}), array(0))")
    ).withColumn("salt", explode("salt_array")).drop("salt_array").alias("e1")

    # Salt outgoing edges from the hub
    # If e2.src == hub, assign deterministic salt: hash(src, dst) % N
    # Else salt = 0
    e2_salted = edges.withColumn(
        "salt",
        expr(f"IF(src == {hub_id}, abs(hash(src, dst)) % {N}, 0)")
    ).alias("e2")

    # Perform salted join
    salted_join = e1_salted.join(e2_salted, (col("e1.dst") == col("e2.src")) & (col("e1.salt") == col("e2.salt")))
    
    salted_count = salted_join.count()
    print(f"--- Salted Join ---")
    print(f"Row count: {salted_count}")
    salted_join.show()
    
    if baseline_count == salted_count:
        print("SUCCESS: Salted join returned the exact same number of rows!")
    else:
        print("ERROR: Row count mismatch. Salting logic is dropping or duplicating rows.")

    spark.stop()

if __name__ == "__main__":
    main()
