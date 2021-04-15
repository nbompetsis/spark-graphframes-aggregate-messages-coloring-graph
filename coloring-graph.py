"""
Simple example using aggregateMessages.

The goal is to find the minimum rating of a player (node) by sending the neighbors
the currently known minimum rating by each node.

Run with: ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/coloring-graph.py

"""

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("colorign-graph-app").getOrCreate()

vertices = spark.read.csv("/vagrant/csv/vertices.csv", header=True)
edges = spark.read.options(delimiter="|").csv("/vagrant/csv/edges.csv", header=True)

# Fix graph to be undirected
reverse_edges = edges.selectExpr("dst as src", "src as dst")
edges = edges.union(reverse_edges)

# add default values of local maximal first alg on vertices
# vertices = (
#     vertices.withColumn("value", F.lit(-1))
#     .withColumn("maxima", F.lit(False))
#     .withColumn("step", F.lit(0))
# )

def new_value(id, value, maxima, step):
    return {"id": id, "value": value, "maxima": maxima, "step": step}


vertice_new_value = types.StructType(
    [
        types.StructField("id", types.StringType()),
        types.StructField("value", types.IntegerType()),
        types.StructField("maxima", types.BooleanType()),
        types.StructField("step", types.IntegerType()),
    ]
)
vertice_new_value_udf = F.udf(new_value, vertice_new_value)

# Add LocalMaximaValue of each node
# vertices = vertices.withColumn(
#     "new_value",
#     vertice_new_value_udf(
#         vertices["id"], vertices["value"], vertices["maxima"], vertices["step"]
#     ),
# )
vertices = vertices.withColumn(
    "localMaximaValue",
    vertice_new_value_udf(vertices["id"], F.lit(-1), F.lit(False), F.lit(1)),
)

cached_vertices = AM.getCachedDataFrame(vertices)

# Create and print information on the respective GraphFrame
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()
g.degrees.show()