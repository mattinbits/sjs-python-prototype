import com.typesafe.config.{ConfigRenderOptions, Config, ConfigFactory}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import py4j.GatewayServer
import scala.sys.process.{ProcessLogger, Process}

class PythonSparkJobEndPoint(val context: JavaSparkContext, val sparkConf: SparkConf, val jobConfig: Config) {

  var result: Option[Any] = None

  def setResult(r: Any):Unit = {
    result = Some(r)
  }

  lazy val jobConfigAsHocon = jobConfig.root().render(ConfigRenderOptions.concise())
}

object SparkGatewayMain extends App {

  //This would be the jobConfig which is constructed in SJS for jobs already:
  val conf = ConfigFactory.parseString(
    """
      |input.items = ['a', 'b', 'c']
    """.stripMargin)

  //Need to construct the python path to include pyspark since we are launching it manually:
  val sparkHome = sys.env.getOrElse("SPARK_HOME", throw new Exception("You need to set SPARK_HOME"))
  val originalPythonPath = sys.env.getOrElse("PYTHONPATH","")
  val newPythonPath = Seq(s"$sparkHome/python", originalPythonPath).mkString(":")

  //Would normally be built by the context factory in SJS:
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("python-example")
  val context = new SparkContext(sparkConf)

  //Passing an endpoint into the Gateway Server gives the Python side of Py4J
  //an single target object to pull artifacts from:
  val endpoint = new PythonSparkJobEndPoint(context, sparkConf, conf)

  //Start a gateway server. Using 0 means it will pick a randomly available high port number
  //this is similar to how Spark starts a gateway for PySpark normally.
  val server = new GatewayServer(endpoint, 0)
  //Server runs asynchronously on a dedicated thread. See Py4J source for more detail
  server.start()

  //Launch the python subprocess. We need to pass the Gateway Server port as a parameter so that
  // the Python code knows where to find the Gateway Server:
  val logger = ProcessLogger(o => println(s"STDOUT: $o"), e => println(s"STDERR: $e"))
  val env = Seq("PYTHONPATH" -> newPythonPath)
  val proc = Process(Seq("python", "src/main/python/client.py", server.getListeningPort.toString), None, env:_*)
  val exitCode = proc.!<(logger)
  println(s"Python process exited with code ${exitCode}")
  server.shutdown()
  println(endpoint.result.getOrElse("NO RESULT"))
}
