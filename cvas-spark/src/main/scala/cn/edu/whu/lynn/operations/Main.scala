/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.whu.lynn.operations

import cn.edu.whu.lynn.common.{ButterflyOptions, CLIOperation}
import cn.edu.whu.lynn.util.OperationHelper
import cn.edu.whu.lynn.util.OperationHelper.{ParsedCommandLineOptions, printOperationUsage}
import org.apache.spark.SparkConf
import org.apache.spark.cvas.{CRSServer, SparkSQLRegistration}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * A main class that runs all supported operations from the command line
  */
object Main extends Logging {

  def main(args: Array[String]): Unit = {
    // Get the operation to run
    if (args.length == 0) {
      OperationHelper.printUsage(System.err)
      throw new RuntimeException("No arguments")
    }

    val parsedCLO: ParsedCommandLineOptions = OperationHelper.parseCommandLineArguments(args: _*)
    if (parsedCLO == null) {
      OperationHelper.printUsage(System.err)
      throw new RuntimeException("No command chosen")
    }

    // Check if the parameters are invalid
    if (!OperationHelper.checkOptions(parsedCLO, System.err)) {
      printOperationUsage(parsedCLO.operation, parsedCLO.options, System.err)
      throw new RuntimeException("Invalid parameters")
    }

    // Create the Spark context
    val conf = new SparkConf
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setAppName("Beast/" + parsedCLO.operation.metadata.shortName)

    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    logInfo(s"Using master '${conf.get("spark.master")}'")

    val opInstance: CLIOperation =
      try {
        // 1- Test the operation as a Scala operation
        // See: https://stackoverflow.com/questions/1913092/getting-object-instance-by-string-name-in-scala
        val opClass = Class.forName(parsedCLO.operation.klass.getName)
        opClass.getField("MODULE$").get(opClass).asInstanceOf[CLIOperation]
      } catch {
        // 2- Fall back to Java operation
        case _: Exception => parsedCLO.operation.klass.asSubclass(classOf[CLIOperation]).newInstance
      }
    var sparkSession: SparkSession = null
    var t1: Long = System.nanoTime()
    try {
      sparkSession = SparkSession.builder().config(conf).getOrCreate()
      try {
        SparkSQLRegistration.registerUDT
        SparkSQLRegistration.registerUDF(sparkSession)
      } catch {
        case e: Throwable => logError(s"Error initializing SparkSQL functions.", e)
      }
      val sparkContext = sparkSession.sparkContext
      // Start the CRSServer and store the information as local properties
      CRSServer.startServer(sparkContext)

      t1 = System.nanoTime

      val opts: ButterflyOptions = parsedCLO.options
      opInstance.setup(opts)
      opInstance.run(opts, parsedCLO.inputs, parsedCLO.outputs, sparkContext)
      val t2 = System.nanoTime
      logInfo(f"The operation ${parsedCLO.operation.metadata.shortName} finished in ${(t2 - t1) * 1E-9}%f seconds")
    } catch {
      case _other: Exception =>
        val t2 = System.nanoTime
        logError(f"The operation ${parsedCLO.operation.metadata.shortName} failed after ${(t2 - t1) * 1E-9}%f seconds")
        throw _other
    } finally {
      if (sparkSession != null) sparkSession.stop()
    }
  }
}
