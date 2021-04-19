# Graph coloring using GraphFrames of Apache Spark framework

## Graph coloring
This repo holds a implementation of a distributed graph processing algorithm finding a solution to graph coloring problem. The minimum number of colors required to properly color a graph is called **chromatic number** of that graph. Finding the chromatic number of a graph is a well-known NP-Hard problem. Furthermore, it is not possible to approximate the chromatic number of a graph into a considerable bound.

In this example, we use a polynomial-time approximation using the Local Maxima First algorithm. In addition, Apache Spark framework and specifically GraphFrames library used to implement and execute the algorithm (described below) that computes a solution to the graph coloring problem and associates each node with a number representing the color that should be used.

## Local Maxima First Algorithm
Follows the Pregel-like pseudocode of Local Maxima First algorithm.

```
begin:
    Set Maxima = true;
    if superstep == 0 then
        Set Vertex.Value = -1;
        Send Vertex.Id to all neighbors;
    else
        if Vertex.Value == -1 then
            foreach msg ∈ Messages do
                if Vertex.Id < msg then
                    Set Maxima = false;
            
            if Maxima==true then
                Set Vertex.Value = superstep;
            else
                Send Vertex.Id to all neighbors;
    VoteToHalt();
```

## Local environment and execution
You can run and experiment locally with the Local Maxima First algorithm executing the coloring-graph.py script on the provided [Vagrant environment](https://www.vagrantup.com) using the following commands.

### Run example
```
$ vagrant up
$ vagrant ssh

# Using default input graph
$ ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11 /vagrant/coloring-graph.py

# Or using different input files
$ ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/coloring-graph.py -v {path-to-vertices.csv} -e {path-to-edges.csv}

$ vagrant halt
```

### CSV Files
The default input graph (G=(V, E)) is providing by the vertices.csv and edges.csv files located under the corresponding /csv folder.

### Graph Coloring Solution
By finishing the execution of the algorithm you will receive the final GraphFrame output.
```
+---+----------------+
| id|     localMaxima|
+---+----------------+
|  7| [7, 2, true, 7]|
| 11|[11, 1, true, 7]|
|  3| [3, 1, true, 7]|
|  8| [8, 1, true, 7]|
|  5| [5, 4, true, 7]|
|  6| [6, 3, true, 7]|
|  9| [9, 1, true, 7]|
|  1| [1, 6, true, 7]|
| 10|[10, 2, true, 7]|
|  4| [4, 5, true, 7]|
|  2| [2, 2, true, 7]|
+---+----------------+
``` 

Each node of the graph contains a dictionary with the following structure.
```
{
    "id" => The id of the node,
    "color" => The color of the node. Initial value -1 (no color),
    "maxima" => Indicates if the node is the maxima of its neighborhood. Initial value False,
    "step" => The Step of the algorithm
}
```

## Credits
This work inspired by the GraphFrames-aggregateMessages [repository](https://github.com/panagiotisl/spark-graphframes-aggregate-messages-examples).