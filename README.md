# CompGraph
CompGraph is a library for creating and processing computational graphs. We define a computational graph as a predefined sequence of operations that can be applied to various datasets (we use a [MapReduce model](https://en.wikipedia.org/wiki/MapReduce)). Computational graphs allow to separate the definition of a sequence of operations from their execution. Due to this, you can both
run operations in another environment (for example, describe a graph in a python interpreter, and then execute it on a Graphics processing unit),
and independently and in a parallel run on multiple machines of a computing cluster to process a large array of
input data for an optimal finite time (for example, this is how the client works for the [Spark](https://en.wikipedia.org/wiki/Apache_Spark) distributed computing system. See [here](https://spark.apache.org/examples.html)).


## Installing
Clone the library to your local machine, then install the library
    
    $ pip install -e compgraph --force-reinstall

You can import the compgraph into your environment now.

## Testing
Example of running all tests
    
    $ pytest compgraph


You can get acquainted with some examples of using CompGraph in the `examples` folder, where we tested a few real solutions

    $ python3 examples/run_word_count.py resources/road_graph_data.txt resources/output.txt

To use the script above, you need to extract `resources/extract_me.tgz`
