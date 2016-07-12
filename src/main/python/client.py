from py4j.java_gateway import JavaGateway, java_import, GatewayClient
import sys
from pyspark.context import SparkContext, SparkConf
from pyhocon import ConfigFactory


class PythonJob(object):
    """
    A prototype of representing an SJS job in Python.
    Specific jobs would override this class.
    """

    def __init__(self, context, jobConf):
        self.jobConf = jobConf
        self.context = context

    def runJob(self):
        """
        Specific jobs should override this method
        with the logic to run the job.
        :return: Any type which can:
        a) Be successfully sent from Python to Java via Py4J AND
        b) Can be successfully serialized by Spray JSON

        For a) note that we use 'autoconvert=true' on the JVM side,
        therefore the autoconversion rules listed here are relevant:
        https://www.py4j.org/advanced_topics.html#converting-python-collections-to-java-collections
        """
        pass

class SpecificPythonJob(PythonJob):

    def runJob(self):
        rdd = sc.parallelize(['a', 'b', 'c']).map(lambda x: x + "_1")
        results = rdd.collect()
        return results


if __name__ == "__main__":
    port = int(sys.argv[1])
    gateway = JavaGateway(GatewayClient(port=port), auto_convert=True)
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    entry_point = gateway.entry_point
    jobConfig = ConfigFactory.parse_string(entry_point.jobConfigAsHocon())
    jsc =  entry_point.context()
    jsparkConf = entry_point.sparkConf()
    sparkConf = SparkConf(_jconf = jsparkConf)
    sc = SparkContext(gateway = gateway, jsc = jsc, conf = sparkConf)
    job = SpecificPythonJob(sc, jobConfig)
    result = job.runJob()
    entry_point.setResult(result)
