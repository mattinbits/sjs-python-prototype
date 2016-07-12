name := "sjs-python-prototype"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "net.sf.py4j" % "py4j" % "0.9"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

fork in run := true

cancelable in Global := true