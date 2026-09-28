import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, expr, explode, sequence, lit

def create_spark():
    spark = (
        SparkSession.builder.appName("BitcoinTrustChain")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def analyze_topology_simple(edges, p=100):
    # Calculate degrees
    out_deg = edges.groupBy("src").count().withColumnRenamed("src", "id").withColumnRenamed("count", "out_deg")
    in_deg = edges.groupBy("dst").count().withColumnRenamed("dst", "id").withColumnRenamed("count", "in_deg")
    degrees = out_deg.join(in_deg, "id", "full").fillna(0)
    degrees = degrees.withColumn("work", col("in_deg") * col("out_deg"))
    
    # Identify hubs (e.g., top 100 vertices by work)
    hub_threshold = degrees.approxQuantile("work", [0.95], 0.01)[0]
    hubs = degrees.filter(col("work") >= hub_threshold)
    
    # Calculate hypercube dimensions: r = sqrt(d_in * P / d_out), s = sqrt(d_out * P / d_in)
    import pyspark.sql.functions as F
    hub_meta = hubs.withColumn("r_raw", F.sqrt((col("in_deg") * p) / (col("out_deg") + 1))) \
                   .withColumn("s_raw", F.sqrt((col("out_deg") * p) / (col("in_deg") + 1)))
                   
    # Simplified bounding for dimensions
    hub_meta = hub_meta.withColumn("r_final", F.greatest(F.round(col("r_raw")).cast("int"), lit(1))) \
                       .withColumn("s_final", F.greatest(F.round(col("s_raw")).cast("int"), lit(1)))
                       
    hub_ids = [row["id"] for row in hub_meta.select("id").collect()]
    return hub_meta, hub_ids

def run_hypercube_motif(edges, hub_meta, hub_ids):
    # 1. Hubs processing
    e1_hub = edges.join(broadcast(hub_meta), edges["dst"] == hub_meta["id"]).select(
        edges["src"], edges["dst"], hub_meta["r_final"].alias("r"), hub_meta["s_final"].alias("s")
    )
    e1_salted = e1_hub.withColumn("r_coord", expr("abs(hash(src)) % r")) \
                      .withColumn("s_coord", explode(sequence(lit(0), col("s") - 1))) \
                      .select("src", "dst", "r_coord", "s_coord").alias("e1h")

    e2_hub = edges.join(broadcast(hub_meta), edges["src"] == hub_meta["id"]).select(
        edges["src"], edges["dst"], hub_meta["r_final"].alias("r"), hub_meta["s_final"].alias("s")
    )
    e2_salted = e2_hub.withColumn("s_coord", expr("abs(hash(dst)) % s")) \
                      .withColumn("r_coord", explode(sequence(lit(0), col("r") - 1))) \
                      .select("src", "dst", "r_coord", "s_coord").alias("e2h")

    hub_join = e1_salted.join(e2_salted, 
        (col("e1h.dst") == col("e2h.src")) & 
        (col("e1h.r_coord") == col("e2h.r_coord")) & 
        (col("e1h.s_coord") == col("e2h.s_coord"))
    ).select(col("e1h.src").alias("A"), col("e1h.dst").alias("B"), col("e2h.dst").alias("C"))
    
    # 2. Non-hubs processing
    e1_non_hub = edges.filter(~col("dst").isin(hub_ids)).alias("e1n")
    e2_non_hub = edges.filter(~col("src").isin(hub_ids)).alias("e2n")
    
    non_hub_join = e1_non_hub.join(e2_non_hub, col("e1n.dst") == col("e2n.src")) \
                             .select(col("e1n.src").alias("A"), col("e1n.dst").alias("B"), col("e2n.dst").alias("C"))
                             
    return hub_join.unionByName(non_hub_join)

def main():
    if os.name == "nt":
        hadoop_home = os.path.join(os.getcwd(), "hadoop")
        if os.path.exists(hadoop_home):
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    print("Initializing Spark...")
    spark = create_spark()
    
    print("Loading Bitcoin OTC dataset...")
    # Schema: SOURCE, TARGET, RATING, TIME
    raw_df = spark.read.csv("soc-sign-bitcoinotc.csv").toDF("src", "dst", "rating", "time")
    
    # Filter for "Trust" edges (Rating > 0)
    trust_edges = raw_df.filter(col("rating") > 0).select(col("src").cast("int"), col("dst").cast("int")).cache()
    
    edge_count = trust_edges.count()
    print(f"Total Trust Edges (Rating > 0): {edge_count:,}")
    
    print("Analyzing Topology & Generating Hypercube Dimensions...")
    hub_meta, hub_ids = analyze_topology_simple(trust_edges, p=100)
    print(f"Identified {len(hub_ids)} heavy-hitter hubs.")
    
    print("Executing 2-Hop Motif Query: (A) -> [trusts] -> (B) -> [trusts] -> (C)")
    t0 = time.time()
    
    # Run Motif
    trust_chains = run_hypercube_motif(trust_edges, hub_meta, hub_ids).cache()
    chain_count = trust_chains.count()
    
    print(f"Done! Found {chain_count:,} Indirect Trust Chains in {time.time() - t0:.2f} seconds.\n")
    
    print("Example Question: 'I am User 6. Who can I trust indirectly through someone I already trust?'")
    user_6_chains = trust_chains.filter(col("A") == 6)
    
    # Remove direct connections (we want indirect ONLY)
    direct_trusts = trust_edges.filter(col("src") == 6).select("dst").rdd.flatMap(lambda x: x).collect()
    
    indirect_only = user_6_chains.filter(~col("C").isin(direct_trusts))
    
    print(f"User 6 trusts {len(direct_trusts)} people directly.")
    print("Here are 5 people User 6 can trust INDIRECTLY via a 2-hop chain:")
    indirect_only.select("A", "B", "C").show(5)

if __name__ == "__main__":
    main()
