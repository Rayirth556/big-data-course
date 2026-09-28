import urllib.request
import urllib.parse
import json
import time

queries = [
    "GraphFrames: An Integrated API for Mixing Graph and Relational Queries",
    "Adaptive Query Execution: Speeding Up Spark SQL at Runtime",
    "Optimizing Joins in a MapReduce Environment Afrati",
    "Optimizing Multiway Joins in a Map-Reduce Environment",
    "SharesSkew: An Algorithm to Handle Skew for Joins in MapReduce",
    "An Effective Algorithm for Parallelizing Sort-Merge Joins in the Presence of Data Skew DeWitt",
    "Skew in Parallel Query Processing Beame",
    "Considering Data Skew Factor in Multi-way Join Query Optimization for Parallel Execution",
    "Scaling and Load-Balancing Equi-Joins Metwally",
    "PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs",
    "Distributed Power-law Graph Computing: Theoretical and Empirical Analysis",
    "GraphPi: High Performance Graph Pattern Matching through Effective Redundancy Elimination",
    "Dryadic: Flexible and Fast Graph Pattern Matching at Scale",
    "GLogS: Interactive Graph Pattern Matching Query at Large Scale",
    "Hybrid Edge Partitioner: Partitioning Large Power-Law Graphs under Memory Constraints"
]

results = []
for q in queries:
    url = "https://api.semanticscholar.org/graph/v1/paper/search?query=" + urllib.parse.quote(q) + "&limit=1&fields=title,authors,year,venue,citationCount,url"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            if data.get('data'):
                results.append(data['data'][0])
            else:
                results.append({"query": q, "error": "Not found"})
    except Exception as e:
        results.append({"query": q, "error": str(e)})
    time.sleep(1)

with open("papers_verified.json", "w") as f:
    json.dump(results, f, indent=2)
