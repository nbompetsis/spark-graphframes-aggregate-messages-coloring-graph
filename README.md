# Graph coloring using GraphFrames of Apache Spark framework

## Graph coloring
This repo holds a implementation of using a distributed graph processing algorithm to associate a solution to graph coloring problem. The minimum number of colors required to properly color a graph is called a chromatic number of that graph. Finding the chromatic number of a graph is a well-known NP-Hard problem. Furthermore, it is not possible to approximate the chromatic number of a graph into a considerable bound.

In this example the boundaries of the chromatic number reduced in order to provide a polynomial-time solution. In addition, Apache Spark framework and specifically GraphFrames library used to implement an iterative Prgel-like algorithm (Local Maxima First) that computes a solution to the graph coloring problem and associates each node with a number representing the color that should be used.

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
You can run and experiment locally with the coloring graph solution of Local Maxima First algorithm using the coloring-graph.py script executing on the provided [Vagrant environment](https://www.vagrantup.com).

### Run example
```
$ vagrant up
$ vagrant ssh
$ ~/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11  /vagrant/coloring-graph.py
$ vagrant halt
```

### CSV Files
The example processes the graph G=(V, E) defined by the vertices.csv and edges.csv files located under the corresponding /csv folder.

## Credits
This repo inspired by the GraphFrames-aggregateMessages [repository](https://github.com/panagiotisl/spark-graphframes-aggregate-messages-examples).