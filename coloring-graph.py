"""
Simple example using aggregateMessages of GraphFrames (a graph processing library) which is part of Apache Spark framework.

The goal is to use the minimum number of colors to properly color a connected graph using different color between neighbors.
More information can be found here: https://en.wikipedia.org/wiki/Graph_coloring

In this example used Local Maxima First algorithm (a Pregel-like algorithm) to associate each node of graph with a number (starting from 1) representing the color that should be used.

Run code:
~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/coloring-graph.py

"""

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F
import argparse

inputVertices = "/vagrant/csv/vertices.csv"
inputEdges = "/vagrant/csv/edges.csv"

parser = argparse.ArgumentParser()
parser.add_argument("-v", help="Vertices csv file")
parser.add_argument("-e", help="Edges csv file")

args = parser.parse_args()
if args.v and args.e:
    inputVertices = args.v
    inputEdges = args.e

spark = SparkSession.builder.appName("colorign-graph-app").getOrCreate()
# Reading vertices and edges csv files
vertices = spark.read.csv(inputVertices, header=True)
edges = spark.read.options(delimiter="|").csv(inputEdges, header=True)

# Fix graph to be undirected
reverse_edges = edges.selectExpr("dst as src", "src as dst")
edges = edges.union(reverse_edges)

# Create local maxima value
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

# Create localMaxima of each node
# LocalMaxima consists of a dictionary with the following structure
# {
#     "id" => The id of the node,
#     "color" => The color of the node. Default value -1 (no color),
#     "maxima" => Indicates if the node is the maxima of its neighborhood
#     "step" => The Step of the algorithm
# }
vertices = vertices.withColumn(
    "localMaxima",
    local_max_value_type_udf(
        vertices["id"].cast("int"), F.lit(-1), F.lit(False), F.lit(1)
    ),
)

# Create and print information on the respective GraphFrame
cached_vertices = AM.getCachedDataFrame(vertices)
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()
g.degrees.show()

# UDF for the neighbor with the greater local maxima value of each node.
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

# Local Maxima First Algorithm
while True:
    # Aggregate messages from the neighbors.
    aggregates = g.aggregateMessages(
        F.collect_set(AM.msg).alias("agg"), sendToDst=AM.src["localMaxima"]
    )
    res = aggregates.withColumn(
        "newlocalMaxima", greater_local_max_value_udf("agg")
    ).drop("agg")

    # Aggregate and Join vertices leveraging localMaxima values
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

    # Vote-To-Halt step.
    # If all vertices have been colored the algorithm stops otherwise continues for another round.
    existUncoloredVertices = (
        False
        if (
            cached_new_vertices.select(
                F.col("localMaxima").getItem("maxima").alias(str("maxima"))
            )
            .filter(F.col("maxima").contains(False))
            .count()
        )
        == 0
        else True
    )

    if existUncoloredVertices != True:
        print("Local Maxima First Algorithm Completed")
        break
