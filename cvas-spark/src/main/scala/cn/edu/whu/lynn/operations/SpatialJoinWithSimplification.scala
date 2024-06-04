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

import cn.edu.whu.lynn._
import cn.edu.whu.lynn.common.{ButterflyOptions, CLIOperation}
import cn.edu.whu.lynn.core._
import cn.edu.whu.lynn.core.index.STRTreeFeatureIndex
import cn.edu.whu.lynn.core.index.quadSplitedGeom.QuadSplitedGeom
import cn.edu.whu.lynn.geolite.{EnvelopeNDLite, Feature, IFeature}
import cn.edu.whu.lynn.indexing.{CellPartitioner, GridPartitioner, IndexHelper, RSGrovePartitioner}
import cn.edu.whu.lynn.io.{SpatialFileRDD, SpatialOutputFormat, SpatialWriter}
import cn.edu.whu.lynn.synopses.{Summary, Synopsis, UniformHistogram}
import cn.edu.whu.lynn.util.{OperationMetadata, OperationParam}
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{HashPartitioner, SparkContext}

import java.io.IOException
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@OperationMetadata(shortName = "sj",
  description = "Computes spatial join that finds all overlapping features from two files.",
  inputArity = "2",
  outputArity = "?",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat]))
object SpatialJoinWithSimplification extends CLIOperation with Logging {

  @OperationParam(description = "The spatial join algorithm to use {'bnlj', 'dj', 'pbsm' = 'sjmr'}.", defaultValue = "auto")
  val SpatialJoinMethod: String = "method"

  @OperationParam(description = "The spatial predicate to use in the join. Supported predicates are {intersects, contains}", defaultValue = "intersects")
  val SpatialJoinPredicate: String = "predicate"

  @OperationParam(description = "Overwrite the output file if it exists", defaultValue = "true")
  val OverwriteOutput: String = "overwrite"

  @OperationParam(description = "Write the output to a file", defaultValue = "true", showInUsage = false)
  val WriteOutput: String = "output"

  @OperationParam(description = "Desired workload (in bytes) per task for spatial join algorithms",
    defaultValue = "32m", showInUsage = false)
  val JoinWorkloadUnit: String = "sjworkload"

  @OperationParam(description = "The maximum ratio of replication in PBSM",
    defaultValue = "0.02", showInUsage = false)
  val ReplicationBound: String = "pbsm.replication"

  @OperationParam(description = "A multiplier that calculates the size of the PBSM grid",
    defaultValue = "200", showInUsage = false)
  val PBSMMultiplier = "pbsmmultiplier"

  @OperationParam(description = "The partitioner to use with PBSM",
    defaultValue = "grid", showInUsage = false)
  val PBSMPartitioner = "pbsmpartitioner"

  @OperationParam(description =
    """
The average number of points per geometry after which geometries will be broken down using
the quad split algorithm to speedup the spatial join
""",
    defaultValue = "1000", showInUsage = false)
  val QuadSplitThreshold: String = "quadsplitthreshold"
  val QuadSplitter: String = "quadsplitter"
  val QuadSplitMethod: String = "quadsplitmethod"
  val FilterMethod: String = "filtermethod"
  /** Instructs spatial join to not keep the geometry of the left dataset (for efficiency) */
  val RemoveGeometry1: String = "SpatialJoin.RemoveGeometry1"

  /** Instructs spatial join to not keep the geometry of the right dataset (for efficiency) */
  val RemoveGeometry2: String = "SpatialJoin.RemoveGeometry2"

  /** The name of the accumulator that records the total number of MBRTests */
  val MBRTestsAccumulatorName = "MBRTests"

  @throws(classOf[IOException])
  override def run(opts: ButterflyOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Unit = {
    // Set the split size to 16MB so that the code will not run out of memory
    sc.hadoopConfiguration.setLong("mapred.max.split.size", opts.getSizeAsBytes(JoinWorkloadUnit, "4m"))
    val mbrTests = sc.longAccumulator(MBRTestsAccumulatorName)
    val joinPredicate = opts.getEnumIgnoreCase(SpatialJoinPredicate, SJPredicate.Intersects)
    val f1rdd = sc.spatialFile(inputs(0), opts.retainIndex(0))
    val f2rdd = sc.spatialFile(inputs(1), opts.retainIndex(1))
    val joinResults = spatialJoin(f1rdd, f2rdd, joinPredicate, null, mbrTests, opts)
    if (opts.getBoolean(WriteOutput, true)) {
      val outPath = new Path(outputs(0))
      // Delete output file if exists and overwrite flag is on
      val fs = outPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(outPath) && opts.getBoolean(SpatialWriter.OverwriteOutput, false))
        fs.delete(outPath, true)
      val resultSize = sc.longAccumulator("resultsize")
      joinResults.map(f1f2 => {
        resultSize.add(1)
        f1f2._1.toString + f1f2._2.toString
      }).saveAsTextFile(outputs(0))
      logInfo(s"Join result size is ${resultSize.value}")
    } else {
      // Skip writing the output. Join count
      val resultSize = joinResults.count()
      logInfo(s"Join result size is ${resultSize}")
    }
    logInfo(s"Total number of MBR tests is ${mbrTests.value}")
  }

  def spatialJoinWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate = SJPredicate.Intersects,
                            joinMethod: SJAlgorithm = null, metrics: MetricsAccumulator = null,
                            opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    val removeGeometry1 = opts.getBoolean(RemoveGeometry1, false)
    val removeGeometry2 = opts.getBoolean(RemoveGeometry2, false)
    val joinWorkloadUnit: Long = opts.getSizeAsBytes(JoinWorkloadUnit, "64m")
    val quadSplitThreshold: Int = opts.getInt(QuadSplitThreshold, 1000)
    val splitter: String = opts.getString(QuadSplitter, "normal")
    val adaptiveVal: Int = opts.getInt("adaptiveVal", 256)
    val splitMethod = opts.getString(QuadSplitMethod, "index")
    var _r1 = r1
    var _r2 = r2
    var quadSplit1: Boolean = false
    var quadSplit2: Boolean = false
    if (quadSplitThreshold > 0) {
      val t1 = System.nanoTime()
      // Check if we need to split one or two of the inputs
      val datasets: Array[(SpatialRDD, Boolean)] = Array((r1, removeGeometry1), (r2, removeGeometry2)).map(rr => Future {
        var features = rr._1
        val summary = if (joinMethod == SJAlgorithm.PBSM || joinMethod == SJAlgorithm.SJMR) {
          // PBSM will need the synopsis in the future. Create it to keep it cached.
          Synopsis.getOrCompute(features).summary
        } else {
          // Full synopsis will not be needed. Just compute the summary and keep it cached.
          features.summary
        }
        if (features.getNumPartitions < summary.size / joinWorkloadUnit &&
          features.getNumPartitions < features.sparkContext.defaultParallelism &&
          !features.isSpatiallyPartitioned)
          features = features.repartition((summary.size / joinWorkloadUnit).toInt)
        val shouldSplit: Boolean = summary.numNonEmptyGeometries > 0 && quadSplitThreshold > 0 &&
          (summary.numPoints / summary.numNonEmptyGeometries) > quadSplitThreshold
        if (shouldSplit) {
          logInfo(s"The dataset has a geometric complexity of ${summary.numPoints / summary.numNonEmptyGeometries} points/geometry." +
            s" Applying quad split with threshold $quadSplitThreshold")
          if (splitter.equals("adaptive")) {
            if (splitMethod.equals("index")) {
              features = GeometryAdaptiveSplitter.adapSplitRDDToIndexedGeom(features, quadSplitThreshold, !rr._2, adaptiveVal)
            } else {
              features = GeometryAdaptiveSplitter.adapSplitRDD(features, quadSplitThreshold, !rr._2, adaptiveVal)
            }
          }
          else {
            if (splitMethod.equals("index")) {
              features = GeometryQuadSplitter.splitRDDToIndexedGeom(features, quadSplitThreshold, !rr._2)
            } else {
              features = GeometryQuadSplitter.splitRDD(features, quadSplitThreshold, !rr._2)
            }
          }
        }
        (features, shouldSplit)
      }).map(x => Await.result(x, Duration.Inf))
      _r1 = datasets(0)._1
      quadSplit1 = datasets(0)._2
      _r2 = datasets(1)._1
      quadSplit2 = datasets(1)._2
      val t2 = System.nanoTime()
      if (quadSplit1 || quadSplit2) {
        logInfo(s"Quad split using ${splitMethod} method took ${(t2 - t1) / 1E9} seconds")
      }
      if (metrics != null && metrics.getQuadSplitTime != null)
        metrics.getQuadSplitTime.add((t2 - t1) / 1E9)
    }

    // 自动选择算法
    val joinAlgorithm = if (joinMethod != null) {
      logDebug(s"Use SJ algorithm $joinMethod as provided by user")
      joinMethod
    } else if (r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm DJ because both inputs are partitioned")
      SJAlgorithm.DJ
    } else if (r1.getNumPartitions * r2.getNumPartitions < r1.sparkContext.defaultParallelism) {
      logDebug("Use SJ algorithm BNLJ because the total number of partitions is less than parallelism " +
        s"${r1.getNumPartitions * r2.getNumPartitions} < ${r1.sparkContext.defaultParallelism}")
      SJAlgorithm.BNLJ
    } else if (r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm RepJ because one input is partitioned")
      SJAlgorithm.REPJ
    } else {
      logDebug("Use SJ algorithm PBSM because inputs are not partitioned")
      SJAlgorithm.PBSM
    }
    logInfo(s"Use SJ algorithm $joinAlgorithm")

    val t3 = System.nanoTime()
    val inputsPartitioned: Boolean = _r1.isSpatiallyPartitioned && _r2.isSpatiallyPartitioned
    val mbrCount = if (metrics != null) metrics.getMbrCount else null
    var joinResult: RDD[(IFeature, IFeature)] = joinAlgorithm match {
      case SJAlgorithm.BNLJ =>
        spatialJoinBNLJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.PBSM | SJAlgorithm.SJMR =>
        spatialJoinPBSMWithMetric(_r1, _r2, joinPredicate, metrics)
      case SJAlgorithm.DJ if inputsPartitioned =>
        spatialJoinDJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.DJ =>
        logWarning("Cannot run DJ with non-partitioned input. Falling back to BNLJ")
        spatialJoinBNLJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.REPJ =>
        spatialJoinRepJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.SJ1D =>
        spatialJoin1DPartitioningWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.BJ =>
        spatialJoinBJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case _other => throw new RuntimeException(s"Unrecognized spatial join method ${_other}. " +
        s"Please specify one of {'pbsm'='sjmr', 'dj', 'repj', 'bnlj','bj'}")
    }
    if (splitMethod.equals("index")) {
      joinResult.map(f1f2 => {
        var f1 = f1f2._1
        if (f1.isInstanceOf[QuadSplitedGeom]) {
          f1 = Feature.create(f1, f1.asInstanceOf[QuadSplitedGeom].getOriginal)
        }
        var f2 = f1f2._2
        if (f2.isInstanceOf[QuadSplitedGeom]) {
          f2 = Feature.create(f2, f2.asInstanceOf[QuadSplitedGeom].getOriginal)
        }
        (f1, f2)
      })
    }
    else {
      if (quadSplit1 || removeGeometry1) {
        // Remove the smaller geometry from the first dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._1
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (new Feature(values.toArray, StructType(schema)), f1f2._2)
        })
      }
      if (quadSplit2 || removeGeometry2) {
        // Remove the smaller geometry from the second dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._2
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (f1f2._1, new Feature(values.toArray, StructType(schema)))
        })
      }
    }
    val t4 = System.nanoTime()
    logInfo(s"Spatial join took ${(t4 - t3) / 1E9} seconds")
    if (metrics != null && metrics.getJoinTime != null)
      metrics.getJoinTime.add((t4 - t3) / 1E9)
    joinResult
  }

  def spatialJoinWithMetricBak(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate = SJPredicate.Intersects,
                               joinMethod: SJAlgorithm = null, metrics: MetricsAccumulator = null,
                               opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    val removeGeometry1 = opts.getBoolean(RemoveGeometry1, false)
    val removeGeometry2 = opts.getBoolean(RemoveGeometry2, false)
    // TODO exp 1 数据并行度
    // TODO exp 2 数据规模
    // TODO exp 3 分区大小
    val joinWorkloadUnit: Long = opts.getSizeAsBytes(JoinWorkloadUnit, "32m")
    val quadSplitThreshold: Int = opts.getInt(QuadSplitThreshold, 1000)
    val splitter: String = opts.getString(QuadSplitter, "normal")
    val adaptiveVal: Int = opts.getInt("adaptiveVal", 256)
    // split方法会有重复，优化为使用index方法
    val splitMethod = opts.getString(QuadSplitMethod, "index")

    // 自动选择算法
    // If the joinMethod is not set, choose an algorithm automatically according to the following rules
    val joinAlgorithm = if (joinMethod != null) {
      logDebug(s"Use SJ algorithm $joinMethod as provided by user")
      joinMethod
    } else if (r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm DJ because both inputs are partitioned")
      SJAlgorithm.DJ
    } else if (r1.getNumPartitions * r2.getNumPartitions < r1.sparkContext.defaultParallelism) {
      logDebug("Use SJ algorithm BNLJ because the total number of partitions is less than parallelism " +
        s"${r1.getNumPartitions * r2.getNumPartitions} < ${r1.sparkContext.defaultParallelism}")
      SJAlgorithm.BNLJ
    } else if (r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm RepJ because one input is partitioned")
      SJAlgorithm.REPJ
    } else {
      logDebug("Use SJ algorithm PBSM because inputs are not partitioned")
      SJAlgorithm.PBSM
    }

    // Store the two inputs in variable to allow modification
    var _r1 = r1
    var _r2 = r2

    var quadSplit1: Boolean = false
    var quadSplit2: Boolean = false

    if (quadSplitThreshold > 0) {
      val t1 = System.nanoTime()
      // Check if we need to split one or two of the inputs
      val datasets: Array[(SpatialRDD, Boolean)] = Array((r1, removeGeometry1), (r2, removeGeometry2)).map(rr => Future {
        var features = rr._1
        val summary = if (joinMethod == SJAlgorithm.PBSM || joinMethod == SJAlgorithm.SJMR) {
          // PBSM will need the synopsis in the future. Create it to keep it cached.
          Synopsis.getOrCompute(features).summary
        } else {
          // Full synopsis will not be needed. Just compute the summary and keep it cached.
          features.summary
        }
        if (features.getNumPartitions < summary.size / joinWorkloadUnit &&
          features.getNumPartitions < features.sparkContext.defaultParallelism &&
          !features.isSpatiallyPartitioned)
          features = features.repartition((summary.size / joinWorkloadUnit).toInt)
        val shouldSplit: Boolean = summary.numNonEmptyGeometries > 0 && quadSplitThreshold > 0 &&
          (summary.numPoints / summary.numNonEmptyGeometries) > quadSplitThreshold
        if (shouldSplit) {
          logInfo(s"The dataset has a geometric complexity of ${summary.numPoints / summary.numNonEmptyGeometries} points/geometry." +
            s" Applying quad split with threshold $quadSplitThreshold")
          if (splitter.equals("adaptive")) {
            if (splitMethod.equals("index")) {
              features = GeometryAdaptiveSplitter.adapSplitRDDToIndexedGeom(features, quadSplitThreshold, !rr._2, adaptiveVal)
            } else {
              features = GeometryAdaptiveSplitter.adapSplitRDD(features, quadSplitThreshold, !rr._2, adaptiveVal)
            }
          }
          else {
            if (splitMethod.equals("index")) {
              features = GeometryQuadSplitter.splitRDDToIndexedGeom(features, quadSplitThreshold, !rr._2)
            } else {
              features = GeometryQuadSplitter.splitRDD(features, quadSplitThreshold, !rr._2)
            }
          }
        }
        (features, shouldSplit)
      }).map(x => Await.result(x, Duration.Inf))
      _r1 = datasets(0)._1
      quadSplit1 = datasets(0)._2
      _r2 = datasets(1)._1
      quadSplit2 = datasets(1)._2
      val t2 = System.nanoTime()
      if (quadSplit1 || quadSplit2) {
        logInfo(s"Quad split using ${splitMethod} method took ${(t2 - t1) / 1E9} seconds")
      }
      if (metrics != null && metrics.getQuadSplitTime != null)
        metrics.getQuadSplitTime.add((t2 - t1) / 1E9)
    }

    val t3 = System.nanoTime()
    val inputsPartitioned: Boolean = _r1.isSpatiallyPartitioned && _r2.isSpatiallyPartitioned
    val mbrCount = if (metrics != null) metrics.getMbrCount else null
    // Run the spatial join algorithm
    // TODO 局部增加基于索引的连接
    var joinResult: RDD[(IFeature, IFeature)] = joinAlgorithm match {
      case SJAlgorithm.BNLJ =>
        spatialJoinBNLJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.PBSM | SJAlgorithm.SJMR =>
        spatialJoinPBSMWithMetric(_r1, _r2, joinPredicate, metrics)
      case SJAlgorithm.DJ if inputsPartitioned =>
        spatialJoinDJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.DJ =>
        logWarning("Cannot run DJ with non-partitioned input. Falling back to BNLJ")
        spatialJoinBNLJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.REPJ =>
        spatialJoinRepJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.SJ1D =>
        spatialJoin1DPartitioningWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case SJAlgorithm.BJ =>
        spatialJoinBJWithMetric(_r1, _r2, joinPredicate, opts, metrics)
      case _other => throw new RuntimeException(s"Unrecognized spatial join method ${_other}. " +
        s"Please specify one of {'pbsm'='sjmr', 'dj', 'repj', 'bnlj'}")
    }
    if (splitMethod.equals("index")) {
      joinResult.map(f1f2 => {
        var f1 = f1f2._1
        if (f1.isInstanceOf[QuadSplitedGeom]) {
          f1 = Feature.create(f1, f1.asInstanceOf[QuadSplitedGeom].getOriginal)
        }
        var f2 = f1f2._2
        if (f2.isInstanceOf[QuadSplitedGeom]) {
          f2 = Feature.create(f2, f2.asInstanceOf[QuadSplitedGeom].getOriginal)
        }
        (f1, f2)
      })
    }
    else {
      if (quadSplit1 || removeGeometry1) {
        // Remove the smaller geometry from the first dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._1
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (new Feature(values.toArray, StructType(schema)), f1f2._2)
        })
      }
      if (quadSplit2 || removeGeometry2) {
        // Remove the smaller geometry from the second dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._2
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (f1f2._1, new Feature(values.toArray, StructType(schema)))
        })
      }
    }
    val t4 = System.nanoTime()
    logInfo(s"Spatial join took ${(t4 - t3) / 1E9} seconds")
    if (metrics != null && metrics.getJoinTime != null)
      metrics.getJoinTime.add((t4 - t3) / 1E9)
    joinResult
  }

  private def spatialJoinBJWithMetric(_r1: SpatialRDD, _r2: SpatialRDD, joinPredicate: SJPredicate, opts: ButterflyOptions, metrics: MetricsAccumulator): RDD[(IFeature, IFeature)] = {
    logInfo("Spatial join using BJ")
    // Build R-tree index for r2
    val rtree = new STRTreeFeatureIndex()
    _r2.collect().foreach(rtree.insert)
    val sc = _r1.sparkContext

    // Sample and partition r1
    val synopsis1 = Synopsis.getOrCompute(_r1)
    val intersectionMBR = synopsis1.summary
    val totalSize = synopsis1.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numCells: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val partitioner = new GridPartitioner(intersectionMBR, numCells)
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(_r1, partitioner)
    r1Partitioned.repartition(sc.defaultParallelism)
    // Broadcast the R-tree index
    val broadcastIndex = sc.broadcast(rtree)

    // Perform spatial join
    val joined: RDD[(IFeature, IFeature)] = r1Partitioned.flatMap {
      case (partitionID, currentDriver) =>
        val rtreeQueryResult: java.util.List[IFeature] = broadcastIndex.value.query(currentDriver)
        import scala.collection.JavaConverters._
        val refine: ((_ <: IFeature, _ <: IFeature)) => Boolean = refiner(joinPredicate)
        val scalaRtreeQueryResult: Iterable[IFeature] = rtreeQueryResult.asScala
        scalaRtreeQueryResult.filter(refine(currentDriver, _)).map((currentDriver, _))
    }
    // 去重
    joined.distinct()
    // Unbroadcast R-tree index
    broadcastIndex.unpersist()
    joined
  }


  def spatialJoinSelfJoinBL(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, metrics: MetricsAccumulator = null,
                            opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("分析阶段", "分析采样数据特征")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Merge the two samples into one
    val sample1 = synopsis1.sample
    val sample2 = synopsis2.sample
    val sample = new Array[Array[Double]](sample1.length)
    for (d <- sample1.indices) {
      sample(d) = if (!sample1.isEmpty && !sample2.isEmpty)
        sample1(d) ++ sample2(d)
      else if (!sample1.isEmpty)
        sample1(d)
      else if (!sample2.isEmpty)
        sample2(d)
      else
        Array[Double]()
    }

    // Remove samples that are not in the intersection MBR
    var sampleSize = 0
    for (i <- sample(0).indices) {
      var pointInside: Boolean = true
      var d = 0
      while (d < sample.length && pointInside) {
        pointInside = sample(d)(i) >= intersectionMBR.getMinCoord(d) && sample(d)(i) < intersectionMBR.getMaxCoord(d)
        d += 1
      }
      if (pointInside && sampleSize < i) {
        for (d <- sample.indices)
          sample(d)(sampleSize) = sample(d)(i)
        sampleSize += 1
      }
    }

    // Shrink the array by removing all filtered out elements
    for (d <- sample.indices)
      sample(d) = sample(d).slice(0, sampleSize)

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    // TODO exp1 更改size和cell个数
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numCells: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    logDebug(s"PBSM is using $numCells cells")
    // TODO exp2 换用其他分区器
    // 分区器参数："cells" "grid" "hcurve" "kdtree" "rgrove" "rrsgrove" "rsgrove" "str" "zcurve"
    val pbsmPartitioner: String = opts.getString(PBSMPartitioner, "grid")
    val partitionerClass: Class[_ <: SpatialPartitioner] = IndexHelper.partitioners(pbsmPartitioner)
    val partitioner = partitionerClass.newInstance()
    partitioner.setup(opts, true)
    val intersectionSummary = new Summary()
    intersectionSummary.set(intersectionMBR)
    val intersectionHistogram = new UniformHistogram(intersectionMBR, 128, 128)
    intersectionHistogram.mergeNonAligned(synopsis1.histogram.asInstanceOf[UniformHistogram])
    intersectionHistogram.mergeNonAligned(synopsis2.histogram.asInstanceOf[UniformHistogram])
    partitioner.construct(intersectionSummary, sample, intersectionHistogram, numCells)
    logInfo(s"SSMJ is using partitioner $partitioner with $numCells cells")

    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (partitioner.numPartitions()) max sc.defaultParallelism max 1
    //    val numPartitions: Int = (r1.getNumPartitions+r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("空间连接阶段", s"算法使用 ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metrics)
    })
  }

  def selfJoinBL(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: SJPredicate,
                 joinMethod: SJAlgorithm, metricsAccumulator: MetricsAccumulator,
                 opts: ButterflyOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinSelfJoinBL(r1.rdd, r2.rdd, joinPredicate, metricsAccumulator, opts))

  def spatialJoinPBSMWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, metrics: MetricsAccumulator = null,
                                opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("分析阶段", "分析采样数据特征")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Merge the two samples into one
    val sample1 = synopsis1.sample
    val sample2 = synopsis2.sample
    val sample = new Array[Array[Double]](sample1.length)
    for (d <- sample1.indices) {
      sample(d) = if (!sample1.isEmpty && !sample2.isEmpty)
        sample1(d) ++ sample2(d)
      else if (!sample1.isEmpty)
        sample1(d)
      else if (!sample2.isEmpty)
        sample2(d)
      else
        Array[Double]()
    }

    // Remove samples that are not in the intersection MBR
    var sampleSize = 0
    for (i <- sample(0).indices) {
      var pointInside: Boolean = true
      var d = 0
      while (d < sample.length && pointInside) {
        pointInside = sample(d)(i) >= intersectionMBR.getMinCoord(d) && sample(d)(i) < intersectionMBR.getMaxCoord(d)
        d += 1
      }
      if (pointInside && sampleSize < i) {
        for (d <- sample.indices)
          sample(d)(sampleSize) = sample(d)(i)
        sampleSize += 1
      }
    }

    // Shrink the array by removing all filtered out elements
    for (d <- sample.indices)
      sample(d) = sample(d).slice(0, sampleSize)

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    // TODO exp1 更改size和cell个数
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numCells: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    logDebug(s"PBSM is using $numCells cells")
    // TODO exp2 换用其他分区器
    // 分区器参数："cells" "grid" "hcurve" "kdtree" "rgrove" "rrsgrove" "rsgrove" "str" "zcurve"
    val pbsmPartitioner: String = opts.getString(PBSMPartitioner, "grid")
    val partitionerClass: Class[_ <: SpatialPartitioner] = IndexHelper.partitioners(pbsmPartitioner)
    val partitioner = partitionerClass.newInstance()
    partitioner.setup(opts, true)
    val intersectionSummary = new Summary()
    intersectionSummary.set(intersectionMBR)
    val intersectionHistogram = new UniformHistogram(intersectionMBR, 128, 128)
    intersectionHistogram.mergeNonAligned(synopsis1.histogram.asInstanceOf[UniformHistogram])
    intersectionHistogram.mergeNonAligned(synopsis2.histogram.asInstanceOf[UniformHistogram])
    partitioner.construct(intersectionSummary, sample, intersectionHistogram, numCells)
    logInfo(s"SSMJ is using partitioner $partitioner with $numCells cells")

    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    println(r1.getNumPartitions, r2.getNumPartitions, sc.defaultParallelism)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("空间连接阶段", s"算法使用 ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metrics)
    })
  }


  /**
   * The main entry point for spatial join operations.
   * Performs a spatial join between the given two inputs and returns an RDD of pairs of matching features.
   * This method is a transformation. However, if the [[SJAlgorithm.PBSM]] is used, the MBR of the two
   * inputs has to be calculated first which runs a reduce action on each dataset even if the output of the spatial
   * join is not used.
   *
   * You can specify a specific spatial join method through the [[joinMethod]] parameter. If not specified, an
   * algorithm will be picked automatically based on the following rules.
   *  - If both datasets are spatially partitioned, the distributed join [[SJAlgorithm.DJ]] algorithm is used.
   *  - If the product of the number of partitions of both datasets is less than [[SparkContext.defaultParallelism]],
   *    then the block nested loop join is used [[SJAlgorithm.BNLJ]]
   *  - If at least one dataset is partition, then the repartition join is used [[SJAlgorithm.REPJ]]
   *  - If none of the above, then the partition based spatial merge join is used [[SJAlgorithm.PBSM]]
   *
   * @param r1            the first (left) dataset
   * @param r2            the second (right) dataset
   * @param joinPredicate the join predicate. The default is [[SJPredicate.Intersects]] which finds all non-disjoint
   *                      features
   * @param joinMethod    the join algorithm. If not specified the algorithm automatically chooses an algorithm based
   *                      on the heuristic described above.
   * @param mbrCount      an (optional) accumulator to count the number of MBR tests during the algorithm.
   * @return an RDD that contains pairs of matching features.
   */
  def spatialJoin(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate = SJPredicate.Intersects,
                  joinMethod: SJAlgorithm = null, mbrCount: LongAccumulator = null,
                  opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    val removeGeometry1 = opts.getBoolean(RemoveGeometry1, false)
    val removeGeometry2 = opts.getBoolean(RemoveGeometry2, false)
    val joinWorkloadUnit: Long = opts.getSizeAsBytes(JoinWorkloadUnit, "32m")
    val quadSplitThreshold: Int = opts.getInt(QuadSplitThreshold, 1000)

    // 自动选择算法
    // If the joinMethod is not set, choose an algorithm automatically according to the following rules
    val joinAlgorithm = if (joinMethod != null) {
      logDebug(s"Use SJ algorithm $joinMethod as provided by user")
      joinMethod
    } else if (r1.isSpatiallyPartitioned && r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm DJ because both inputs are partitioned")
      SJAlgorithm.DJ
    } else if (r1.getNumPartitions * r2.getNumPartitions < r1.sparkContext.defaultParallelism) {
      logDebug("Use SJ algorithm BNLJ because the total number of partitions is less than parallelism " +
        s"${r1.getNumPartitions * r2.getNumPartitions} < ${r1.sparkContext.defaultParallelism}")
      SJAlgorithm.BNLJ
    } else if (r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned) {
      logDebug("Use SJ algorithm RepJ because one input is partitioned")
      SJAlgorithm.REPJ
    } else {
      logDebug("Use SJ algorithm PBSM because inputs are not partitioned")
      SJAlgorithm.PBSM
    }

    // Store the two inputs in variable to allow modification
    var _r1 = r1
    var quadSplit1: Boolean = false
    var _r2 = r2
    var quadSplit2: Boolean = false

    val t1 = System.nanoTime()

    if (quadSplitThreshold > 0) {
      // Check if we need to split one or two of the inputs
      val datasets: Array[(SpatialRDD, Boolean)] = Array((r1, removeGeometry1), (r2, removeGeometry2)).map(rr => Future {
        var features = rr._1
        val summary = if (joinMethod == SJAlgorithm.PBSM || joinMethod == SJAlgorithm.SJMR) {
          // PBSM will need the synopsis in the future. Create it to keep it cached.
          Synopsis.getOrCompute(features).summary
        } else {
          // Full synopsis will not be needed. Just compute the summary and keep it cached.
          features.summary
        }
        if (features.getNumPartitions < summary.size / joinWorkloadUnit &&
          features.getNumPartitions < features.sparkContext.defaultParallelism &&
          !features.isSpatiallyPartitioned)
          features = features.repartition((summary.size / joinWorkloadUnit).toInt)
        val shouldSplit: Boolean = summary.numNonEmptyGeometries > 0 && quadSplitThreshold > 0 &&
          (summary.numPoints / summary.numNonEmptyGeometries) > quadSplitThreshold
        if (shouldSplit) {
          logInfo(s"The dataset has a geometric complexity of ${summary.numPoints / summary.numNonEmptyGeometries} points/geometry." +
            s" Applying quad split with threshold $quadSplitThreshold")
          features = GeometryQuadSplitter.splitRDD(features, quadSplitThreshold, !rr._2)
          // If we do a quad split, the summary is no longer accurate as it does not reflect the correct
          // number of polygons, number of points, or average side length
          // However, we don't recompute it to avoid the additional overhead in case the file is very big
        }
        (features, shouldSplit)
      }).map(x => Await.result(x, Duration.Inf))
      _r1 = datasets(0)._1
      quadSplit1 = datasets(0)._2
      _r2 = datasets(1)._1
      quadSplit2 = datasets(1)._2
    }
    val t2 = System.nanoTime()
    if (quadSplit1 || quadSplit2) {
      logInfo(s"Quad split took ${(t2 - t1) / 1E9} seconds")
    }
    val t3 = System.nanoTime()
    val inputsPartitioned: Boolean = _r1.isSpatiallyPartitioned && _r2.isSpatiallyPartitioned

    // Run the spatial join algorithm
    // TODO 局部增加基于索引的连接
    var joinResult: RDD[(IFeature, IFeature)] = joinAlgorithm match {
      case SJAlgorithm.BNLJ =>
        spatialJoinBNLJ(_r1, _r2, joinPredicate, mbrCount)
      case SJAlgorithm.PBSM | SJAlgorithm.SJMR =>
        spatialJoinPBSM(_r1, _r2, joinPredicate, mbrCount, opts)
      case SJAlgorithm.DJ if inputsPartitioned =>
        spatialJoinDJ(_r1, _r2, joinPredicate, mbrCount)
      case SJAlgorithm.DJ =>
        logWarning("Cannot run DJ with non-partitioned input. Falling back to BNLJ")
        spatialJoinBNLJ(_r1, _r2, joinPredicate, mbrCount)
      case SJAlgorithm.REPJ =>
        spatialJoinRepJ(_r1, _r2, joinPredicate, mbrCount)
      case SJAlgorithm.SJ1D =>
        spatialJoin1DPartitioning(_r1, _r2, joinPredicate, opts, mbrCount)
      case _other => throw new RuntimeException(s"Unrecognized spatial join method ${_other}. " +
        s"Please specify one of {'pbsm'='sjmr', 'dj', 'repj', 'bnlj'}")
    }
    // 后处理
    if (quadSplit1 || quadSplit2) {
      if (quadSplit1 || removeGeometry1) {
        // Remove the smaller geometry from the first dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._1
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (new Feature(values.toArray, StructType(schema)), f1f2._2)
        })
      }
      if (quadSplit2 || removeGeometry2) {
        // Remove the smaller geometry from the second dataset
        joinResult = joinResult.map(f1f2 => {
          val f = f1f2._2
          var values: Seq[Any] = Row.unapplySeq(f).get
          values = values.slice(1, values.length)
          var schema: Seq[StructField] = f.schema
          schema = schema.slice(1, schema.length)
          (f1f2._1, new Feature(values.toArray, StructType(schema)))
        })
      }
    }

    val t4 = System.nanoTime()
    logInfo(s"Spatial join took ${(t4 - t3) / 1E9} seconds")
    val throughput = joinResult.count() / ((t4 - t3) / 1E9)
    joinResult
  }

  /**
   * Spatial join for two JavaRDDs. A Java shortcut for [[spatialJoin()]]
   *
   * @param r1            first spatial RDD (left)
   * @param r2            second spatial RDD (right)
   * @param joinPredicate the spatial predicate for the join
   * @param joinMethod    the algorithm to use in the join. If null, an algorithm is auto-selected based on the inputs.
   * @param mbrCount      an optional accumulator to keep track of the number of MBR tests
   * @param opts          additional options to further customize the join algorithm
   * @return
   */
  def spatialJoin(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: SJPredicate,
                  joinMethod: SJAlgorithm, mbrCount: LongAccumulator,
                  opts: ButterflyOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoin(r1.rdd, r2.rdd, joinPredicate, joinMethod, mbrCount, opts))

  def spatialJoinWithMetric(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: SJPredicate,
                            joinMethod: SJAlgorithm, metricsAccumulator: MetricsAccumulator,
                            opts: ButterflyOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinWithMetric(r1.rdd, r2.rdd, joinPredicate, joinMethod, metricsAccumulator, opts))

  /**
   * Runs a plane-sweep algorithm between the given two arrays of input features and returns an iterator of
   * pairs of features.
   *
   * @param r               the first set of features
   * @param s               the second set of features
   * @param dupAvoidanceMBR the duplicate avoidance MBR to run the reference point technique.
   * @param joinPredicate   the join predicate to match features
   * @param numMBRTests     an (optional) accumulator to count the number of MBR tests
   * @tparam T1 the type of the first dataset
   * @tparam T2 the type of the second dataset
   * @return an iterator over pairs of features
   */
  def spatialJoinIntersectsPlaneSweepFeatures[T1 <: IFeature, T2 <: IFeature]
  (r: Array[T1], s: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, joinPredicate: SJPredicate,
   numMBRTests: LongAccumulator): TraversableOnce[(IFeature, IFeature)] = {
    if (r.isEmpty || s.isEmpty)
      return Seq()
    logDebug(s"Joining ${r.length} x ${s.length} records using planesweep")
    val refine: ((_ <: IFeature, _ <: IFeature)) => Boolean = refiner(joinPredicate)
    new PlaneSweepSpatialJoinIterator(r, s, dupAvoidanceMBR, numMBRTests)
      .filter(refine)
  }

  def filterAndRefine[T1 <: IFeature, T2 <: IFeature]
  (r: Array[T1], s: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, joinPredicate: SJPredicate, opts: ButterflyOptions = new ButterflyOptions(),
   metricsAccumulator: MetricsAccumulator = null): TraversableOnce[(IFeature, IFeature)] = {
    if (r.isEmpty || s.isEmpty)
      return Seq()
    logDebug(s"Joining ${r.length} x ${s.length} records using planesweep")
    var filterMethod: String = opts.getString(FilterMethod, "planesweep")
    val refine: ((_ <: IFeature, _ <: IFeature)) => Boolean = refiner(joinPredicate)
    if (filterMethod.equals("planesweep")) {
      return new PlaneSweepSpatialJoinIteratorWithMetric(r, s, dupAvoidanceMBR, metricsAccumulator)
        .filter(refine)
    }
    else {
      // TODO need test
      return new IndexNestedLoopSpatialJoinIteratorWithMetric(r, s, dupAvoidanceMBR, metricsAccumulator)
        .filter(refine)
    }
  }

  /**
   * Returns the correct refinement function for the given predicate
   *
   * @param joinPredicate
   * @return
   */
  private def refiner(joinPredicate: SJPredicate): ((_ <: IFeature, _ <: IFeature)) => Boolean = joinPredicate match {
    case SJPredicate.Contains => p =>
      try {
        p._1.getGeometry.contains(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: ${p.toString()}", e); false
      }
    case SJPredicate.Intersects => p =>
      try {
        p._1.getGeometry.intersects(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    // For MBR intersects, we still need to check the envelopes since the planesweep algorithm is approximate
    // The planesweep algorithm works only with rectangles
    case SJPredicate.MBRIntersects => p => {
      val env1: EnvelopeNDLite = new EnvelopeNDLite().merge(p._1.getGeometry)
      val env2: EnvelopeNDLite = new EnvelopeNDLite().merge(p._2.getGeometry)
      env1.intersectsEnvelope(env2)
    }
    case SJPredicate.Equals => p => {
      try {
        p._1.getGeometry.equals(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Overlaps => p => {
      try {
        p._1.getGeometry.overlaps(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Touches => p => {
      try {
        p._1.getGeometry.touches(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Crosses => p => {
      try {
        p._1.getGeometry.crosses(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Disjoint => p => {
      try {
        p._1.getGeometry.disjoint(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Within => p => {
      try {
        p._1.getGeometry.within(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException =>
          logWarning(s"Error comparing records. p: $p", e); false
      }
    }
    case SJPredicate.Covers => p => {
      try {
        p._1.getGeometry.covers(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException => logWarning(s"Error comparing records", e); false
      }
    }
    case SJPredicate.Covered_by => p => {
      try {
        p._1.getGeometry.coveredBy(p._2.getGeometry)
      }
      catch {
        case e: RuntimeException => logWarning(s"Error comparing records", e); false
      }
    }
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @param opts          Additional options for the PBSM algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null,
                      opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r1 and r2 for PBSM")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Merge the two samples into one
    val sample1 = synopsis1.sample
    val sample2 = synopsis2.sample
    val sample = new Array[Array[Double]](sample1.length)
    for (d <- sample1.indices) {
      sample(d) = if (!sample1.isEmpty && !sample2.isEmpty)
        sample1(d) ++ sample2(d)
      else if (!sample1.isEmpty)
        sample1(d)
      else if (!sample2.isEmpty)
        sample2(d)
      else
        Array[Double]()
    }

    // Remove samples that are not in the intersection MBR
    var sampleSize = 0
    for (i <- sample(0).indices) {
      var pointInside: Boolean = true
      var d = 0
      while (d < sample.length && pointInside) {
        pointInside = sample(d)(i) >= intersectionMBR.getMinCoord(d) && sample(d)(i) < intersectionMBR.getMaxCoord(d)
        d += 1
      }
      if (pointInside && sampleSize < i) {
        for (d <- sample.indices)
          sample(d)(sampleSize) = sample(d)(i)
        sampleSize += 1
      }
    }

    // Shrink the array by removing all filtered out elements
    for (d <- sample.indices)
      sample(d) = sample(d).slice(0, sampleSize)

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    // TODO exp1 更改size和cell个数
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numCells: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    logDebug(s"PBSM is using $numCells cells")
    // TODO exp2 换用其他分区器
    val pbsmPartitioner: String = opts.getString(PBSMPartitioner, "grid")
    val partitionerClass: Class[_ <: SpatialPartitioner] = IndexHelper.partitioners(pbsmPartitioner)
    val partitioner = partitionerClass.newInstance()
    partitioner.setup(opts, true)
    val intersectionSummary = new Summary()
    intersectionSummary.set(intersectionMBR)
    val intersectionHistogram = new UniformHistogram(intersectionMBR, 128, 128)
    intersectionHistogram.mergeNonAligned(synopsis1.histogram.asInstanceOf[UniformHistogram])
    intersectionHistogram.mergeNonAligned(synopsis2.histogram.asInstanceOf[UniformHistogram])
    partitioner.construct(intersectionSummary, sample, intersectionHistogram, numCells)
    logInfo(s"PBSM is using partitioner $partitioner with $numCells cells")

    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("SpatialJoin", s"PBSM is using ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   (output) the number of MBR tests done during the algorithm
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: SJPredicate,
                      numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinPBSM(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /**
   * Performs a partition-based spatial-merge (PBSM) join as explained in the following paper.
   * Jignesh M. Patel, David J. DeWitt:
   * Partition Based Spatial-Merge Join. SIGMOD Conference 1996: 259-270
   * https://doi.org/10.1145/233269.233338
   *
   * (Java shortcut)
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @return a pair RDD for joined features
   */
  def spatialJoinPBSM(r1: JavaSpatialRDD, r2: JavaSpatialRDD, joinPredicate: SJPredicate)
  : JavaPairRDD[IFeature, IFeature] = spatialJoinPBSM(r1, r2, joinPredicate, null)

  /**
   * Runs a spatial join between the two given RDDs using the block-nested-loop join algorithm.
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @return
   */
  def spatialJoinBNLJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null)
  : RDD[(IFeature, IFeature)] = {
    // Convert the two RDD to arrays
    val f1: RDD[Array[IFeature]] = r1.glom()
    val f2: RDD[Array[IFeature]] = r2.glom()

    // Combine them using the Cartesian product (as in block nested loop)
    val f1f2 = f1.cartesian(f2)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Block-nested loop join with ${f1f2.getNumPartitions} partitions")

    // For each pair of blocks, run the spatial join algorithm
    f1f2.flatMap(p1p2 => {
      // Extract the two arrays of features
      val p1: Array[IFeature] = p1p2._1
      val p2: Array[IFeature] = p1p2._2

      // Duplicate avoidance MBR is set to infinity (include all space) in the BNLJ algorithm
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      dupAvoidanceMBR.setInfinite()
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  def spatialJoinBNLJWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, opts: ButterflyOptions = new ButterflyOptions(), metricsAccumulator: MetricsAccumulator = null)
  : RDD[(IFeature, IFeature)] = {
    // Convert the two RDD to arrays
    val f1: RDD[Array[IFeature]] = r1.glom()
    val f2: RDD[Array[IFeature]] = r2.glom()

    // Combine them using the Cartesian product (as in block nested loop)
    val f1f2 = f1.cartesian(f2)

    r1.sparkContext.setJobGroup("连接连接", s"spatial nested loop join with ${f1f2.getNumPartitions} partitions")

    // For each pair of blocks, run the spatial join algorithm
    f1f2.flatMap(p1p2 => {
      // Extract the two arrays of features
      val p1: Array[IFeature] = p1p2._1
      val p2: Array[IFeature] = p1p2._2

      // Duplicate avoidance MBR is set to infinity (include all space) in the BNLJ algorithm
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      dupAvoidanceMBR.setInfinite()
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metricsAccumulator)
    })
  }

  /** Java shortcut */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: SJPredicate, numMBRTests: LongAccumulator): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, numMBRTests))

  /** Java shortcut without MBR count */
  def spatialJoinBNLJ(r1: JavaSpatialRDD, r2: JavaSpatialRDD,
                      joinPredicate: SJPredicate): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(spatialJoinBNLJ(r1.rdd, r2.rdd, joinPredicate, null))

  /**
   * Distributed join algorithm between spatially partitioned RDDs
   *
   * @param r1            the first set of features
   * @param r2            the second set of features
   * @param joinPredicate the predicate that joins a feature from r1 with a feature in r2
   * @param numMBRTests   a counter that will contain the number of MBR tests
   * @return a pair RDD for joined features
   */
  def spatialJoinDJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null):
  RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned, "r1 should be spatially partitioned")
    require(r2.isSpatiallyPartitioned, "r2 should be spatially partitioned")
    // Skip duplicate avoidance from both input RDDs since this algorithm relies on the replication
    SpatialFileRDD.skipDuplicateAvoidance(r1)
    SpatialFileRDD.skipDuplicateAvoidance(r2)
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")
    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  def spatialJoinDJWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, opts: ButterflyOptions = new ButterflyOptions(), metricsAccumulator: MetricsAccumulator = null):
  RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned, "r1 should be spatially partitioned")
    require(r2.isSpatiallyPartitioned, "r2 should be spatially partitioned")
    // Skip duplicate avoidance from both input RDDs since this algorithm relies on the replication
    SpatialFileRDD.skipDuplicateAvoidance(r1)
    SpatialFileRDD.skipDuplicateAvoidance(r2)
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")
    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metricsAccumulator)
    })
  }

  def optimizedSpatialJoinDJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null):
  RDD[(IFeature, IFeature)] = {

    // 确保 r1 和 r2 都已经进行了空间分区
    require(r1.isSpatiallyPartitioned, "r1 should be spatially partitioned")
    require(r2.isSpatiallyPartitioned, "r2 should be spatially partitioned")

    // Skip duplicate avoidance from both input RDDs since this algorithm relies on the replication
    SpatialFileRDD.skipDuplicateAvoidance(r1)
    SpatialFileRDD.skipDuplicateAvoidance(r2)

    // 利用 R-tree 索引进行连接
    //    val indexedRDD1 = r1.buildIndex(IndexType.RTREE)
    //    val indexedRDD2 = r2.buildIndex(IndexType.RTREE)

    // 使用空间交叉算法进行连接
    val matchingPartitions: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      //      new SpatialIntersectionRDD1(indexedRDD1, indexedRDD2)
      new SpatialIntersectionRDD1(r1, r2)
    r1.sparkContext.setJobGroup("SpatialJoin", s"Distributed join with ${matchingPartitions.getNumPartitions} partitions")

    matchingPartitions.flatMap(joinedPartition => {
      val dupAvoidanceMBR: EnvelopeNDLite = joinedPartition._1
      // Extract the two arrays of features
      val p1: Array[IFeature] = joinedPartition._2._1.toArray
      val p2: Array[IFeature] = joinedPartition._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  /** *
   * Repartition join algorithm between two datasets: r1 is spatially disjoint partitioned and r2 is not
   *
   * @param r1            the first dataset
   * @param r2            the second dataset
   * @param joinPredicate the join predicate
   * @param numMBRTests   an optional accumulator that counts the number of MBR tests
   * @return an RDD of pairs of matching features
   */
  def spatialJoinRepJ(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null):
  RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned,
      "Repartition join requires at least one of the two datasets to be spatially partitioned")
    // Choose which dataset to repartition, 1 for r1, and 2 for r2
    // If only one dataset is partitioned, always repartition the other one
    // If both are partitioned, repartition the smaller one
    val whichDatasetToPartition: Int = if (!r1.isSpatiallyPartitioned)
      1
    else if (!r2.isSpatiallyPartitioned)
      2
    else {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      // Choose the smaller dataset
      r1.sparkContext.setJobGroup("Analyzing", "Estimating the size of r1 and r2 for REPJ")
      val mbr1Async = Future {
        r1.summary
      }
      val mbr2Async = Future {
        r2.summary
      }
      val size1: Long = concurrent.Await.result(mbr1Async, Duration.Inf).size
      val size2: Long = concurrent.Await.result(mbr2Async, Duration.Inf).size
      if (size1 < size2) 1 else 2
    }
    val (r1Partitioned: SpatialRDD, r2Partitioned: SpatialRDD) = if (whichDatasetToPartition == 1) {
      // Repartition r1 according to r2
      val referencePartitioner = new CellPartitioner(r2.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r2)
      val r1Partitioned = r1.spatialPartition(referencePartitioner)
      (r1Partitioned, r2)
    } else {
      // Repartition r2 according to r1
      val referencePartitioner = new CellPartitioner(r1.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r1)
      val r2Partitioned = r2.spatialPartition(referencePartitioner)
      (r1, r2Partitioned)
    }
    val joined: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1Partitioned, r2Partitioned)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Repartition Join with ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val dupAvoidanceMBR: EnvelopeNDLite = r._1
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  def spatialJoinRepJWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate, opts: ButterflyOptions = new ButterflyOptions(), metricsAccumulator: MetricsAccumulator = null):
  RDD[(IFeature, IFeature)] = {
    require(r1.isSpatiallyPartitioned || r2.isSpatiallyPartitioned,
      "Repartition join requires at least one of the two datasets to be spatially partitioned")
    // Choose which dataset to repartition, 1 for r1, and 2 for r2
    // If only one dataset is partitioned, always repartition the other one
    // If both are partitioned, repartition the smaller one
    val whichDatasetToPartition: Int = if (!r1.isSpatiallyPartitioned)
      1
    else if (!r2.isSpatiallyPartitioned)
      2
    else {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      // Choose the smaller dataset
      r1.sparkContext.setJobGroup("Analyzing", "Estimating the size of r1 and r2 for REPJ")
      val mbr1Async = Future {
        r1.summary
      }
      val mbr2Async = Future {
        r2.summary
      }
      val size1: Long = concurrent.Await.result(mbr1Async, Duration.Inf).size
      val size2: Long = concurrent.Await.result(mbr2Async, Duration.Inf).size
      if (size1 < size2) 1 else 2
    }
    val (r1Partitioned: SpatialRDD, r2Partitioned: SpatialRDD) = if (whichDatasetToPartition == 1) {
      // Repartition r1 according to r2
      val referencePartitioner = new CellPartitioner(r2.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r2)
      val r1Partitioned = r1.spatialPartition(referencePartitioner)
      (r1Partitioned, r2)
    } else {
      // Repartition r2 according to r1
      val referencePartitioner = new CellPartitioner(r1.getSpatialPartitioner)
      SpatialFileRDD.skipDuplicateAvoidance(r1)
      val r2Partitioned = r2.spatialPartition(referencePartitioner)
      (r1, r2Partitioned)
    }
    val joined: RDD[(EnvelopeNDLite, (Iterator[IFeature], Iterator[IFeature]))] =
      new SpatialIntersectionRDD1(r1Partitioned, r2Partitioned)

    r1.sparkContext.setJobGroup("SpatialJoin", s"Repartition Join with ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val dupAvoidanceMBR: EnvelopeNDLite = r._1
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metricsAccumulator)
    })
  }

  def spatialJoin1DPartitioning(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate,
                                opts: ButterflyOptions = new ButterflyOptions(),
                                numMBRTests: LongAccumulator = null): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r1 and r2")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val partitioner = new GridPartitioner(intersectionMBR, Array(1, numStrips))
    logInfo(s"SJ1D is using partitioner $partitioner")
    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("SpatialJoin", s"PBSM is using ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      spatialJoinIntersectsPlaneSweepFeatures(p1, p2, dupAvoidanceMBR, joinPredicate, numMBRTests)
    })
  }

  def spatialJoin1DPartitioningWithMetric(r1: SpatialRDD, r2: SpatialRDD, joinPredicate: SJPredicate,
                                          opts: ButterflyOptions = new ButterflyOptions(),
                                          metricsAccumulator: MetricsAccumulator = null): RDD[(IFeature, IFeature)] = {
    // Compute the MBR of the intersection area
    val sc = r1.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r1 and r2 for PBSM")
    val synopsis1 = Synopsis.getOrCompute(r1)
    val synopsis2 = Synopsis.getOrCompute(r2)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    logDebug(s"Summary for right dataset ${synopsis2.summary}")
    val intersectionMBR = synopsis1.summary.intersectionEnvelope(synopsis2.summary)
    if (intersectionMBR.isEmpty)
      return sc.emptyRDD[(IFeature, IFeature)]

    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size + synopsis2.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val partitioner = new GridPartitioner(intersectionMBR, Array(1, numStrips))
    logInfo(s"SJ1D is using partitioner $partitioner")
    // Co-partition both datasets  using the same partitioner
    val r1Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r1, partitioner)
    val r2Partitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r2, partitioner)
    val numPartitions: Int = (r1.getNumPartitions + r2.getNumPartitions) max sc.defaultParallelism max 1
    val joined: RDD[(Int, (Iterable[IFeature], Iterable[IFeature]))] =
      r1Partitioned.cogroup(r2Partitioned, new HashPartitioner(numPartitions))

    sc.setJobGroup("SpatialJoin", s"PBSM is using ${joined.getNumPartitions} partitions")
    joined.flatMap(r => {
      val partitionID = r._1
      val dupAvoidanceMBR = new EnvelopeNDLite(2)
      partitioner.getPartitionMBR(partitionID, dupAvoidanceMBR)
      val p1: Array[IFeature] = r._2._1.toArray
      val p2: Array[IFeature] = r._2._2.toArray
      filterAndRefine(p1, p2, dupAvoidanceMBR, joinPredicate, opts, metricsAccumulator)
    })
  }

  /**
   * Runs a self-join on the given set of data.
   *
   * @param r             the set of features to self-join
   * @param joinPredicate the join predicate
   * @param numMBRTests   an (optional) accumulator to count the number of MBR tests
   * @param options       additional options for the self-join algorithm
   * @return
   */
  def selfJoin(r: SpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator = null, options: ButterflyOptions = new ButterflyOptions()):
  RDD[(IFeature, IFeature)] = {
    val rPartitioned = if (r.isSpatiallyPartitioned && r.getSpatialPartitioner.isDisjoint) r
    else r.spatialPartition(classOf[RSGrovePartitioner], r.getNumPartitions, "disjoint" -> true)
    val partitioner = rPartitioned.getSpatialPartitioner
    rPartitioned.mapPartitionsWithIndex((id, features) => {
      val refPointMBR: EnvelopeNDLite = partitioner.getPartitionMBR(id)
      new PlaneSweepSelfJoinIterator(features.toArray, refPointMBR, numMBRTests)
        .filter(refiner(joinPredicate))
    })
  }

  def buffer(r: SpatialRDD, metricsAccumulator: MetricsAccumulator = null, opts: ButterflyOptions = new ButterflyOptions(), bufferSize: Double = 0.1):
  RDD[IFeature] = {
    val sc = r.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r")
    val synopsis1 = Synopsis.getOrCompute(r)
    logInfo(s"Summary for left dataset ${synopsis1.summary}")
    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val partitioner = new GridPartitioner(synopsis1.summary, Array(1, numStrips))
    val rPartitioned: RDD[(Int, IFeature)] = IndexHelper._assignFeaturesToPartitions(r, partitioner)
    rPartitioned.map(
      idFeatures => {
        val features = idFeatures._2
        // 只是缓冲区生成，不dissolve的话直接用JTS的buffer效果要好一些
        Feature.create(features, features.getGeometry.buffer(bufferSize))
      }
    )
  }

  def buffer(r: JavaSpatialRDD, metricsAccumulator: MetricsAccumulator, opts: ButterflyOptions, bufferSize: Double): JavaRDD[IFeature] = {
    JavaRDD.fromRDD(buffer(r.rdd, metricsAccumulator, opts, bufferSize))
  }

  def selfJoinWithMetric(r: SpatialRDD, joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator = null, opts: ButterflyOptions = new ButterflyOptions()):
  RDD[(IFeature, IFeature)] = {
    val sc = r.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r")
    val synopsis1 = Synopsis.getOrCompute(r)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val keepLeft = opts.getBoolean("keepLeft", false)
    val rPartitioned = if (r.isSpatiallyPartitioned && r.getSpatialPartitioner.isDisjoint) r
    else r.spatialPartition(classOf[GridPartitioner], numStrips, "disjoint" -> true)
    val partitioner = rPartitioned.getSpatialPartitioner
    rPartitioned.mapPartitionsWithIndex((id, features) => {
      val refPointMBR: EnvelopeNDLite = partitioner.getPartitionMBR(id)
      new PlaneSweepSelfJoinIteratorWithMetric(features.toArray, refPointMBR, metricsAccumulator, keepLeft)
        .filter(refiner(joinPredicate))
    })
  }


  def bufferWithDissolve(r: SpatialRDD, joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator = null, opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, List[IFeature])] = {
    val sc = r.sparkContext
    sc.setJobGroup("Analyzing", "Summarizing r")
    val synopsis1 = Synopsis.getOrCompute(r)
    logDebug(s"Summary for left dataset ${synopsis1.summary}")
    // Divide the intersection MBR based on the input sizes assuming 16MB per cell
    val totalSize = synopsis1.summary.size
    val numWorkUnits: Int = (totalSize / opts.getSizeAsBytes(JoinWorkloadUnit, "32m")).toInt max sc.defaultParallelism max 1
    val numStrips: Int = numWorkUnits * opts.getInt(PBSMMultiplier, 200)
    val keepLeft = opts.getBoolean("keepLeft", false)
    val bufferSize = opts.getDouble("bufferdis", 1e-9)
    val rPartitioned = if (r.isSpatiallyPartitioned && r.getSpatialPartitioner.isDisjoint) r
    else r.spatialPartition(classOf[GridPartitioner], numWorkUnits, "disjoint" -> true)
    val partitioner = rPartitioned.getSpatialPartitioner
    val rbuffered = r.map(f => Feature.create(f, f.getGeometry.buffer(bufferSize)))
    // Perform self-join and obtain pairs
    val pairsRDD: RDD[(IFeature, IFeature)] = rbuffered.mapPartitionsWithIndex((id, features) => {
      val refPointMBR: EnvelopeNDLite = partitioner.getPartitionMBR(id)
      new PlaneSweepSelfJoinIteratorWithMetric(features.toArray, refPointMBR, metricsAccumulator, keepLeft)
        .filter(refiner(joinPredicate))
    })

    // Create a graph from the pairs RDD
    val vertices: RDD[(Long, IFeature)] = rbuffered.map(f => (f.hashCode(), f))
    //      val vertices: RDD[(Long, IFeature)] = pairsRDD.flatMap(pair => List(pair._1, pair._2)).distinct().zipWithUniqueId().map { case (feature, id) => (id, feature) }
    val edges: RDD[Edge[Boolean]] = pairsRDD.map { f1f2 =>
      val srcId = f1f2._1.hashCode()
      val dstId = f1f2._2.hashCode()
      Edge(srcId, dstId, true)
    }
    val graph: Graph[IFeature, Boolean] = Graph(vertices, edges)
    // Run connected components algorithm
    val connectedComponents: RDD[(VertexId, VertexId)] = graph.connectedComponents().vertices
    // Group features by connected component ID and collect them into a list
    val groupedFeatures: RDD[(VertexId, List[IFeature])] = connectedComponents.join(vertices).map { case (id, (ccId, feature)) => (ccId, feature) }.groupByKey().mapValues(_.toList)

    // Format the result as (IFeature, List[IFeature])
    groupedFeatures.map { case (_, features) => (features.head, features.tail) }
  }

  def bufferWithDissolve(r: JavaSpatialRDD, joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator, opts: ButterflyOptions): JavaPairRDD[IFeature, java.util.List[IFeature]] = {
    JavaPairRDD.fromRDD(bufferWithDissolve(r.rdd, joinPredicate, metricsAccumulator, opts)).mapValues(_.asJava)
  }
  //  def selfJoinToCC(r: SpatialRDD, pairsRDD: RDD[(IFeature, IFeature)], joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator = null, opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, List[IFeature])] = {
  //    //    val vertices: RDD[(Long, IFeature)] = r.map(feature => (feature.hashCode(), feature))
  //    //    vertices.cache()
  //    // 为每个feature分配一个递增的ID
  //    val sc = r.sparkContext
  //    // 使用zipWithUniqueId来创建节点和ID的映射
  //    val verticesWithIndex: RDD[(IFeature, VertexId)] = r.zipWithUniqueId().map {
  //      case (feature, id) => (feature, id)
  //    }
  //    val vertices: RDD[(VertexId, IFeature)] = verticesWithIndex.map(_.swap)
  //    vertices.cache()
  //
  //    val verticesBroadcast = sc.broadcast(verticesWithIndex.collectAsMap())
  //
  //    // 创建边，使用广播变量来查找节点ID
  //    val edges: RDD[Edge[Boolean]] = pairsRDD.flatMap { case (f1, f2) =>
  //      val maybeSrcId = verticesBroadcast.value.get(f1)
  //      val maybeDstId = verticesBroadcast.value.get(f2)
  //      for {
  //        srcId <- maybeSrcId
  //        dstId <- maybeDstId
  //      } yield Edge(srcId, dstId, true)
  //    }
  //
  //    val graph: Graph[IFeature, Boolean] = Graph(vertices, edges)
  //    // Run connected components algorithm
  //    val connectedComponents: RDD[(VertexId, VertexId)] = graph.connectedComponents(3).vertices
  //    // Group features by connected component ID and collect them into a list
  //    val groupedFeatures: RDD[(VertexId, List[IFeature])] = connectedComponents.join(vertices).map { case (id, (ccId, feature)) => (ccId, feature) }.groupByKey().mapValues(_.toList)
  //
  //    // Format the result as (IFeature, List[IFeature])
  //    groupedFeatures.map { case (_, features) => (features.head, features.tail) }
  //  }

  import org.apache.spark.SparkContext._
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  // Step 1: 创建顶点
  def createVertices(r: SpatialRDD): (RDD[(VertexId, IFeature)], Broadcast[Map[IFeature, VertexId]]) = {
    val sc = r.sparkContext
    val verticesWithIndex: RDD[(IFeature, VertexId)] = r.zipWithUniqueId().map {
      case (feature, id) => (feature, id)
    }
    val vertices: RDD[(VertexId, IFeature)] = verticesWithIndex.map(_.swap)
    vertices.cache()

    val verticesBroadcast = sc.broadcast(verticesWithIndex.collectAsMap().toMap)
    (vertices, verticesBroadcast)
  }

  // Step 2: 创建边
  def createEdges(pairsRDD: RDD[(IFeature, IFeature)], verticesBroadcast: Broadcast[Map[IFeature, VertexId]]): RDD[Edge[Boolean]] = {
    pairsRDD.flatMap { case (f1, f2) =>
      val maybeSrcId = verticesBroadcast.value.get(f1)
      val maybeDstId = verticesBroadcast.value.get(f2)
      for {
        srcId <- maybeSrcId
        dstId <- maybeDstId
      } yield Edge(srcId, dstId, true)
    }
  }

  // Step 3: 构建图
  def constructGraph(vertices: RDD[(VertexId, IFeature)], edges: RDD[Edge[Boolean]]): Graph[IFeature, Boolean] = {
    Graph(vertices, edges)
  }

  // Step 4: 运行连通组件算法
  def runConnectedComponents(graph: Graph[IFeature, Boolean]): RDD[(VertexId, VertexId)] = {
    graph.connectedComponents().vertices
  }

  // Step 5: 分组特征
  def groupFeatures(connectedComponents: RDD[(VertexId, VertexId)], vertices: RDD[(VertexId, IFeature)]): RDD[(IFeature, List[IFeature])] = {
    val groupedFeatures: RDD[(VertexId, List[IFeature])] = connectedComponents.join(vertices).map {
      case (id, (ccId, feature)) => (ccId, feature)
    }.groupByKey().mapValues(_.toList)

    // Step 6: 格式化结果
    groupedFeatures.map { case (_, features) => (features.head, features.tail) }
  }

  // 将上述步骤合并到selfJoinToCC中
  def selfJoinToCC(r: SpatialRDD, pairsRDD: RDD[(IFeature, IFeature)], joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator = null, opts: ButterflyOptions = new ButterflyOptions()): RDD[(IFeature, List[IFeature])] = {
    val (vertices, verticesBroadcast) = createVertices(r)
    val edges = createEdges(pairsRDD, verticesBroadcast)
    val graph = constructGraph(vertices, edges)
    val connectedComponents = runConnectedComponents(graph)
    groupFeatures(connectedComponents, vertices)
  }

  def selfJoinToCC(r: JavaSpatialRDD, pairsRDD: JavaPairRDD[IFeature, IFeature], joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator, opts: ButterflyOptions): JavaPairRDD[IFeature, java.util.List[IFeature]] = {
    JavaPairRDD.fromRDD(selfJoinToCC(r.rdd, pairsRDD.rdd, joinPredicate, metricsAccumulator, opts)).mapValues(_.asJava)
  }


  def selfJoinWithMetric(r: JavaSpatialRDD, joinPredicate: SJPredicate, metricsAccumulator: MetricsAccumulator, options: ButterflyOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(selfJoinWithMetric(r.rdd, joinPredicate, metricsAccumulator, options))

  /**
   * Java shortcut for self-join
   *
   * @param r             the set of features to self-join
   * @param joinPredicate the join predicate
   * @param numMBRTests   an (optional) accumulator to count the number of MBR tests
   * @param options       additional options for the self-join algorithm
   * @return
   */
  def selfJoin(r: JavaSpatialRDD, joinPredicate: SJPredicate, numMBRTests: LongAccumulator, options: ButterflyOptions): JavaPairRDD[IFeature, IFeature] =
    JavaPairRDD.fromRDD(selfJoin(r.rdd, joinPredicate, numMBRTests, options));

}


