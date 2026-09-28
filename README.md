# Topology-Aware Skew Handling for GraphFrames Motif Queries

**Course Project:** 21CSC314P - Big Data Essentials

## Overview
Degree-aware vertex-cut partitioning is a proven technique to mitigate communication and join skew in distributed graph processing. While present in Spark's RDD-based GraphX engine, Spark's DataFrame/SQL-based GraphFrames motif finding engine relies on standard hash-partitioned joins. For power-law graphs with high-degree hubs, this results in severe computational skew ($W(v) = d^-(v) \cdot d^+(v)$) that evades standard Adaptive Query Execution (AQE).

This repository implements and evaluates **Topology-Aware 2D Hypercube Partitioning** for GraphFrames motif queries, balancing Cartesian workload across Spark partitions.

---

## Repository Structure

- `Algorithm.md` - Formal mathematical formulation and algorithm specification for 2D hypercube partitioning.
- `pokec_benchmark.py` - End-to-end benchmark on the Stanford SNAP Pokec social network graph.
- `synthetic_benchmark.py` - Synthetic power-law graph generation and skew benchmarking suite.
- `bitcoin_trust_chain.py` - Multi-hop motif query evaluation on the Bitcoin OTC trust network.
- `print_results.py` & `search_papers.py` - Helper utilities for result processing and literature search.
- `results.md` & `pokec_benchmark_results.json` - Empirical benchmark logs, execution times, and ablation results.
- `literature/` & `papers_verified.json` - Literature review and related work citations.
- `Topology-Aware Skew Handling for GraphFrames Motif Queries - Conference Paper.docx` - Research manuscript / conference paper draft.
- `results.pdf` & `21CSC314P Big Data Essentials Minor Project  PPTTemplate Review 1.pptx.pdf` - Project presentation slides and experimental result reports.

---

## Setup & Execution

### Prerequisites
- Python 3.8+
- Apache Spark 3.x & PySpark
- GraphFrames package (`graphframes:graphframes:0.8.3-spark3.5-s_2.12` or compatible)

### Running Benchmarks
```bash
# Run the Pokec social network benchmark
python pokec_benchmark.py

# Run the synthetic power-law skew benchmark
python synthetic_benchmark.py

# Run Bitcoin OTC trust chain analysis
python bitcoin_trust_chain.py
```
