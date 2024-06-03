package cn.edu.whu.lynn.core

import cn.edu.whu.lynn.core.index.STRTreeFeatureIndex
import cn.edu.whu.lynn.geolite.{EnvelopeNDLite, IFeature}
import org.apache.spark.internal.Logging
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._


/**
 * A class that runs the index nested loop join algorithm and emits records one pair at a time.
 * Used to avoid keeping all pairs in memory before producing the final result.
 * We include the duplicate avoidance testing here since it is more efficient to test when we already know the MBRs.
 */
class IndexNestedLoopSpatialJoinIteratorWithMetric[T1 <: IFeature, T2 <: IFeature]
(var r1: Array[T1], var r2: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, metrics: MetricsAccumulator = null)
  extends Iterator[(T1, T2)] with Logging {
  var numMBRTests: LongAccumulator = null;
  var leftCount: LongAccumulator = null;
  var rightCount: LongAccumulator = null;
  if (metrics != null) {
    numMBRTests = metrics.getMbrCount
    leftCount = metrics.getLeftCount
    rightCount = metrics.getRightCount
  }
  /** A counter that keeps track of the result size. Used for debugging and logging. */
  var count: Long = 0
  var i: Int = 0
  val rtree = new STRTreeFeatureIndex()
  var driver = r1
  var driven = r2
  val endIdx = driver.length
  driven.foreach(f => rtree.insert(f))
  var geometryList: List[T2] = List()

  override def hasNext: Boolean = {
    while (i < endIdx && (geometryList.isEmpty || geometryList.headOption.isEmpty)) {
      // Find the next driver that intersects with any element in the driven array
      val currentDriver = driver(i)
      geometryList = rtree.query(currentDriver).asScala.toList.asInstanceOf[List[T2]]
      // TODO 重复避免逻辑
      // currentDriver包围盒与geometryList元素包围盒交点成为参考点
      // 当参考点落在dupAvoidanceMBR中时，才报告，否则移除
      // 过滤出与参考点相交的重复对象
      geometryList = geometryList.filter { geometry =>
        val intersectionMBR = currentDriver.getGeometry.getEnvelopeInternal.intersection(geometry.getGeometry.getEnvelopeInternal)
        if (intersectionMBR != null && dupAvoidanceMBR.toJTSEnvelope.contains(intersectionMBR.getMaxX, intersectionMBR.getMaxY)) {
          true
        } else {
          false
        }
      }
      i += 1
    }
    geometryList.nonEmpty
  }

  override def next(): (T1, T2) = {
    val result = (driver(i - 1), geometryList.head)
    geometryList = geometryList.tail
    result
  }
}

