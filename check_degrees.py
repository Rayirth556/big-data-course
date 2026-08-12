import os
from pyspark.sql import SparkSession
from graphframes import GraphFrame

def main():
    if os.name == 'nt':
        hadoop_home = os.path.join(os.getcwd(), 'hadoop')
        if os.path.exists(hadoop_home):
            os.environ['HADOOP_HOME'] = hadoop_home
            bin_dir = os.path.join(hadoop_home, 'bin')
            os.environ['PATH'] = bin_dir + os.pathsep + os.environ.get('PATH', '')

    # Set up Spark session
    spark = SparkSession.builder \
        .appName("DegreeDistributionCheck") \
        .master("local[*]") \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    print("Loading data...")
    edges_df = spark.read.csv("soc-sign-bitcoinotc.csv").toDF("src", "dst", "rating", "time")
    
    vertices_src = edges_df.select("src").withColumnRenamed("src", "id")
    vertices_dst = edges_df.select("dst").withColumnRenamed("dst", "id")
    vertices_df = vertices_src.union(vertices_dst).distinct()

    g = GraphFrame(vertices_df, edges_df)
    
    # Calculate degrees
    degrees = g.degrees
    
    print("\n--- Degree Distribution Statistics ---")
    degrees.describe("degree").show()
    
    # Calculate percentiles
    percentiles = degrees.approxQuantile("degree", [0.5, 0.75, 0.9, 0.95, 0.99, 0.999], 0.01)
    
    print("Percentiles:")
    print(f"50th (Median): {percentiles[0]}")
    print(f"75th:          {percentiles[1]}")
    print(f"90th:          {percentiles[2]}")
    print(f"95th:          {percentiles[3]}")
    print(f"99th:          {percentiles[4]}")
    print(f"99.9th:        {percentiles[5]}")
    
    # Count of extreme hubs
    hubs_100 = degrees.filter("degree > 100").count()
    hubs_500 = degrees.filter("degree > 500").count()
    hubs_1000 = degrees.filter("degree > 1000").count()
    
    print(f"\nVertices with degree > 100:  {hubs_100}")
    print(f"Vertices with degree > 500:  {hubs_500}")
    print(f"Vertices with degree > 1000: {hubs_1000}")
    
    spark.stop()

if __name__ == "__main__":
    main()
