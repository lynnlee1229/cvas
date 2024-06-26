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
package cn.edu.whu.lynn.common

import cn.edu.whu.lynn.util.IConfigurable
import java.io.{IOException, PrintStream}
import org.apache.spark.SparkContext;

/**
  * An interface for a command-line operation that runs from Scala
  */
trait CLIOperation extends IConfigurable {

  /**
   * Run the main function using the given user command-line options and spark context
   *
   * @param opts user options for configuring the operation
   * @param inputs inputs provided by the user
   * @param outputs outputs provided by the user
   * @param sc the Spark context used to run the operation
   * @return an optional result of this operation
   */
  @throws(classOf[IOException])
  def run(opts: ButterflyOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any

  /**
   * Print the usage of this class (if any)
   * @param out
   */
  def printUsage(out: PrintStream): Unit = {}
}
