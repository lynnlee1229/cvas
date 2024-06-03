/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.whu.lynn.monitoring

import org.apache.spark.SparkContext
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

object Metrics {
  def createLongMetric(sc: SparkContext, name: String): LongAccumulator = {
    val acc = new LongAccumulator()
    sc.register(acc, "butterfly.algorithm." + name)
    acc
  }
  def createDoubleMetric(sc: SparkContext, name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator()
    sc.register(acc, "butterfly.algorithm." + name)
    acc
  }
}
