## Prototype of supporting python in SJS

This small program demonstrates the following concepts, related to
enabling Python support in [Spark Job Server](https://github.com/spark-jobserver/spark-jobserver):

- How to start a Py4J gateway in Scala
- How to start a SparkContext in a Scala program, then
 launch a Python (Pyspark) program which uses the context created
 by the Scala program rather than creating its own.
- How to make objects from the Scala program available to the Python
program through the Py4J gateway
- How to use HOCON (Typesafe config) for the jobConfig on the Python side
- How to return results from the Python program to the Scala program

To run the prototype, simply run `sbt run`

**NB This code assumes you have `SPARK_HOME` set**

Problems this prototype does not solve:

- How to store several Python Jobs in such a way that we can instruct
the python client on which job to execute at runtime.

### Python dependencies

Works with Python 2 or 3, requires:

    pip install py4j==0.9
    pip install pyhocon
