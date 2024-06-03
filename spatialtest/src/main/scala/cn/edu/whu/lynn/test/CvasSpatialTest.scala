
package cn.edu.whu.lynn.test

import org.apache.spark.cvas.SparkSQLRegistration
import org.apache.spark.sql.SparkSession
import org.apache.spark.test.ScalaSparkTest

/**
 * A mixin for Scala tests that creates a Spark context and adds methods to create an empty scratch directory
 */
trait CvasSpatialTest extends ScalaSparkTest {

  override def sparkSession: SparkSession = {
    val session = super.sparkSession
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(session)
    session
  }
}
