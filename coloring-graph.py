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

# Creates the local maxima value
def local_max_value(id, value, maxima, step):
    return {"id": id, "color": value, "maxima": maxima, "step": step}


local_max_value_type = types.StructType(
    [
        types.StructField("id", types.IntegerType()),
        types.StructField("color", types.IntegerType()),
        types.StructField("maxima", types.BooleanType()),
        types.StructField("step", types.IntegerType()),
    ]
)
local_max_value_type_udf = F.udf(local_max_value, local_max_value_type)

# Add localMaxima of each node
# LocalMaxima consists of a dictionary with following structure
# {
#     "id" => The id of the vertices,
#     "color" => The color of the vertices. Default value -1 (no color),
#     "maxima" => This variable indicates if the vertice is the maxima of its neighborhood
#     "step" => The Step of the algorithm
# }
vertices = vertices.withColumn(
    "localMaxima",
    local_max_value_type_udf(
        vertices["id"].cast("int"), F.lit(-1), F.lit(False), F.lit(1)
    ),
)

cached_vertices = AM.getCachedDataFrame(vertices)

# Create and print information on the respective GraphFrame
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()
g.degrees.show()

# UDF for preserving the local maxima between those received by all neighbors.
def greater_local_max_value(local_max_value_neighbors):
    max_id = -1
    color = -1
    maxima = False
    step = -1
    for neighbor in local_max_value_neighbors:
        if neighbor.maxima == False and neighbor.id > max_id:
            max_id = neighbor.id
            color = neighbor.color
            maxima = neighbor.maxima
            step = neighbor.step
    return {"id": max_id, "color": color, "maxima": maxima, "step": step}


greater_local_max_value_udf = F.udf(greater_local_max_value, local_max_value_type)

# UDF for comparing local maxima between the old one and the new one.
def compare_local_max_value(old_local_max, new_local_max):

    if old_local_max.maxima == True:
        return {
            "id": old_local_max.id,
            "color": old_local_max.color,
            "maxima": old_local_max.maxima,
            "step": (old_local_max.step + 1),
        }

    maxima = False
    color = old_local_max.color
    step = old_local_max.step + 1
    if new_local_max.id < old_local_max.id:
        maxima = True
        color = old_local_max.step

    return {"id": old_local_max.id, "color": color, "maxima": maxima, "step": step}


compare_local_max_value_udf = F.udf(compare_local_max_value, local_max_value_type)

max_iterations = 5
for _ in range(max_iterations):
    aggregates = g.aggregateMessages(
        F.collect_set(AM.msg).alias("agg"), sendToDst=AM.src["localMaxima"]
    )
    res = aggregates.withColumn(
        "newlocalMaxima", greater_local_max_value_udf("agg")
    ).drop("agg")
    new_vertices = (
        g.vertices.join(res, on="id", how="left_outer")
        .withColumnRenamed("localMaxima", "oldlocalMaxima")
        .withColumn(
            "localMaxima",
            compare_local_max_value_udf(
                F.col("oldlocalMaxima"), F.col("newlocalMaxima")
            ),
        )
        .drop("oldlocalMaxima")
        .drop("newlocalMaxima")
    )
    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g = GraphFrame(cached_new_vertices, g.edges)
    g.vertices.show()