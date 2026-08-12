# Proposal: Degree-Aware Graph Partitioning for Optimizing GraphFrames Motif Queries

## Proposed Title

**Degree-Aware Graph Partitioning for Optimizing GraphFrames Motif Queries in Distributed Spark Analytics**

## Short Idea

GraphFrames is a graph analytics library built on top of Spark DataFrames. It is useful because graph data can be queried together with relational/tabular data using the Spark SQL ecosystem. However, GraphFrames motif finding is executed mainly through DataFrame joins. These joins are optimized by Spark SQL/Catalyst as relational operations, but they do not provide the same graph-native physical partitioning model used by GraphX.

GraphX, Spark's older graph processing library, uses a graph-native representation with vertex-cut-style partitioning strategies. This allows edges to be distributed across partitions while high-degree vertices may be replicated, reducing communication cost for many power-law graphs.

Our project proposes a degree-aware, vertex-cut-inspired repartitioning layer for GraphFrames motif workloads. The aim is to reduce shuffle cost, task skew, and runtime for motif queries on skewed transaction graphs while preserving the GraphFrames programming model.

## Problem Statement

Distributed graph workloads often suffer from high communication cost because graph computations require data from connected vertices and edges to meet on the same machine. This issue becomes more serious in power-law graphs, where a few high-degree hub vertices are connected to many low-degree vertices.

GraphFrames expresses motif queries using a high-level graph pattern language, but internally these motif queries are translated into DataFrame joins. Standard DataFrame join planning is powerful, but it is not the same as graph-native edge placement. In skewed graphs, joins involving high-degree vertices can create large shuffle partitions, straggler tasks, and high data movement.

The problem we address is:

> Can a degree-aware edge repartitioning strategy reduce shuffle cost and runtime for GraphFrames motif queries on skewed transaction graphs?

## Existing Systems

### GraphX

GraphX is Spark's graph-native API based on RDDs. It represents graphs using vertex and edge collections and provides graph-specific operators such as `aggregateMessages`, `Pregel`, and graph algorithms. GraphX also supports explicit graph partitioning strategies through `Graph.partitionBy`.

GraphX adopts a vertex-cut approach: edges are assigned to partitions, and vertices may be replicated across partitions when their incident edges are distributed. This is useful for real-world power-law graphs because it avoids forcing all edges of a high-degree hub into one location.

Relevant GraphX partitioning strategies include:

- `RandomVertexCut`
- `CanonicalRandomVertexCut`
- `EdgePartition1D`
- `EdgePartition2D`

`EdgePartition2D` is especially relevant because it uses a 2D partitioning of the sparse adjacency matrix and provides a bound on vertex replication.

### GraphFrames

GraphFrames represents vertices and edges as Spark DataFrames. It supports motif finding, graph algorithms, and integration with Spark SQL/DataFrame pipelines. This makes it attractive for workflows where graph data must be combined with relational data, ETL, machine learning pipelines, and SQL-style analytics.

GraphFrames motif finding lets users write patterns such as:

```python
g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
```

This motif is internally executed as joins over the edge and vertex DataFrames. Spark SQL/Catalyst can optimize these joins, and Spark AQE can handle some forms of join skew. However, this is still relational query optimization, not a GraphX-style graph-native vertex-cut partitioning model.

## Where The Novelty Is

The novelty is not that graph partitioning is new. It is not.

Vertex-cut partitioning, degree-aware partitioning, HDRF, PowerLyra, and GraphX-style partitioning are already established ideas.

The novelty is also not that GraphFrames has no graph-aware optimization. The original GraphFrames paper discusses graph-aware join optimization, view selection, and attribute-based partitioning.

The actual novelty is more specific:

> We propose and evaluate a degree-aware, vertex-cut-inspired repartitioning strategy for GraphFrames motif queries, targeting skewed transaction graphs where high-degree vertices create expensive joins and shuffle imbalance.

This is a useful gap because GraphFrames provides a convenient DataFrame-based programming model, but it does not expose a GraphX-style degree-aware physical graph partitioning engine for motif workloads.

## Why This Is Needed

A natural question is:

> If GraphX already has graph partitioning, why do we need this in GraphFrames?

The answer is that GraphX and GraphFrames serve different use cases.

GraphX is stronger for low-level graph-native computation and graph-parallel algorithms. GraphFrames is useful when the graph is part of a larger Spark SQL/DataFrame workflow. Many real-world pipelines store data as tables, perform ETL with DataFrames, join graph data with external attributes, and then run motif queries or machine learning.

Using GraphFrames allows users to keep the DataFrame programming model. The goal of this project is to improve performance for selected graph workloads without forcing users to move completely to GraphX.

In short:

> The goal is to preserve GraphFrames' usability and Spark SQL integration while recovering some of the performance benefits of graph-aware partitioning.

## Experimental Plan

### Baselines

Compare the proposed method against:

1. Default GraphFrames motif finding
2. GraphFrames with Spark AQE enabled
3. GraphFrames with simple `repartition(src)`
4. GraphFrames with simple `repartition(dst)`
5. Proposed degree-aware repartitioning
6. Optional: GraphX with `EdgePartition2D`

### Metrics

Measure:

- total runtime
- shuffle read bytes
- shuffle write bytes
- remote shuffle read bytes
- spill to memory/disk
- number of tasks
- max task time
- median task time
- task skew ratio
- partition size imbalance

### Datasets

Possible datasets:

- Bitcoin OTC trust network
- Bitcoin Alpha trust network
- larger SNAP social/transaction graphs
- synthetic scaled power-law graph if the real dataset is too small

## Expected Contribution

The expected contribution is an experimental systems prototype showing whether degree-aware repartitioning improves GraphFrames motif query performance on skewed graphs.

The paper can claim:

1. A clear performance problem: GraphFrames motif joins can suffer from shuffle overhead and skew on power-law graphs.
2. A practical user-space optimization: degree-aware repartitioning before motif execution.
3. A comparative evaluation against default GraphFrames and Spark AQE.
4. An analysis of when the method helps and when Spark's own optimizer already handles the workload.

## What We Should Not Claim

We should not claim:

- that graph partitioning itself is new
- that vertex-cut partitioning is new
- that GraphFrames has no graph-aware optimization at all
- that we are fully modifying Spark Catalyst
- that we are replacing GraphX

Instead, we should claim:

> We introduce and evaluate a degree-aware, vertex-cut-inspired repartitioning layer for GraphFrames motif workloads.

## IEEE-Style Research Question

The final research question can be:

> How does degree-aware edge repartitioning affect the runtime, shuffle cost, and task skew of GraphFrames motif queries on power-law transaction graphs?

## Possible Abstract Draft

GraphFrames provides a convenient DataFrame-based interface for graph analytics in Apache Spark, enabling motif queries to be integrated with relational data processing pipelines. However, GraphFrames motif finding is executed through Spark SQL joins, whose physical data placement is not specialized for skewed graph structures. In contrast, GraphX supports graph-native vertex-cut partitioning strategies that reduce communication cost for power-law graphs. This paper proposes a degree-aware edge repartitioning strategy for GraphFrames motif workloads. The method identifies high-degree hub vertices and repartitions edges using a vertex-cut-inspired key before executing motif queries. We evaluate the approach on transaction graph datasets using runtime, shuffle read/write, spill, and task skew metrics. The goal is to determine whether graph-structure-aware repartitioning can improve GraphFrames motif performance while preserving the DataFrame programming model.

## Key References To Use

- Apache Spark GraphX Programming Guide: https://spark.apache.org/docs/3.5.7/graphx-programming-guide.html
- GraphFrames Documentation: https://graphframes.io/
- GraphFrames Motif Finding Documentation: https://graphframes.io/04-user-guide/04-motif-finding.html
- Spark SQL Performance Tuning and AQE: https://spark.apache.org/docs/3.5.5/sql-performance-tuning.html
- SNAP Bitcoin OTC Dataset: https://snap.stanford.edu/data/soc-sign-bitcoin-otc.html
- GraphX Paper: GraphX: Graph Processing in a Distributed Dataflow Framework
- GraphFrames Paper: GraphFrames: An Integrated API for Mixing Graph and Relational Queries
- HDRF: High Degree Replicated First partitioning
- PowerLyra: Differentiated Graph Computation and Partitioning on Skewed Graphs

## Immediate Next Steps

1. Finalize the research question.
2. Select 2-3 datasets.
3. Implement the default GraphFrames motif baseline.
4. Collect Spark SQL physical plans and shuffle metrics.
5. Implement degree computation and hub identification.
6. Implement 2-3 repartitioning strategies.
7. Compare against default GraphFrames and AQE.
8. Prepare tables and graphs for runtime, shuffle, and skew.
9. Write the paper only after results are collected.

