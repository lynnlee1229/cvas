package cn.edu.whu.lynn.core

import cn.edu.whu.lynn.geolite.{EnvelopeNDLite, IFeature}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

/**
 * A class that runs the plane-sweep join algorithm and emits records one pair at a time.
 * Used to avoid keeping all pairs in memory before producing the final result.
 * We include the duplicate avoidance testing here since it is more efficient to test when we already know the MBRs.
 */
class PlaneSweepSpatialJoinIteratorWithMetric[T1 <: IFeature, T2 <: IFeature]
(var r1: Array[T1], var r2: Array[T2], dupAvoidanceMBR: EnvelopeNDLite, metrics: MetricsAccumulator = null)
  extends Iterator[(T1, T2)] with Logging {
  var numMBRTests: LongAccumulator = null;
  var joinPreparationTime: DoubleAccumulator = null;
  var filterTime: DoubleAccumulator = null;
  if (metrics != null) {
    numMBRTests = metrics.getMbrCount
    joinPreparationTime = metrics.getJoinPreparationTime
    filterTime = metrics.getFilterTime
  }
  // 计时变量
  var prepareStartTime: Long = 0
  var prepareEndTime: Long = 0
  var filterStartTime: Long = 0
  var filterEndTime: Long = 0
  /**
   * A comparator that sorts features by xmin which is needed for the planesweep algorithm
   */
  //    (f1, f2) => f1.getGeometry.getEnvelopeInternal.getMinX < f2.getGeometry.getEnvelopeInternal.getMinX
  //  val featureComparator: (IFeature, IFeature) => Boolean =

  val featureComparator: (IFeature, IFeature) => Boolean = {
    (f1, f2) => {
      val x1 = f1.getGeometry.getEnvelopeInternal.getMinX
      val x2 = f2.getGeometry.getEnvelopeInternal.getMinX

      if (x1 == x2) {
        // If x values are equal, compare by y values
        val y1 = f1.getGeometry.getEnvelopeInternal.getMinY
        val y2 = f2.getGeometry.getEnvelopeInternal.getMinY
        y1 < y2
      } else {
        // Compare by x values
        x1 < x2
      }
    }
  }
  // TODO joinPreparationTime不能直接累加，应当用外部总时间减去其他时间，此处相关代码注释弃用
//  prepareStartTime = System.nanoTime
  r1 = r1.sortWith(featureComparator)
  r2 = r2.sortWith(featureComparator)
//  prepareEndTime = System.nanoTime
//  joinPreparationTime.add((prepareEndTime - prepareStartTime) / 1e9)
//  println(s"r1: ${r1.length}, r2: ${r2.length},joinPreparationTime: ${joinPreparationTime.value}")
  /** A counter that keeps track of the result size. Used for debugging and logging. */
  var count: Long = 0

  // Initialize the plane-sweep algorithm and make it ready to emit records
  var i: Int = 0
  var j: Int = 0
  var ii: Int = 0
  var jj: Int = 0

  /** The list that is currently active, either 1 or 2 */
  var activeList: Int = 0

  /** Prepare the first result (if any) */
  seekToNextOutput()

  // Accessor methods for envelope coordinates
  def xmin1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMinX

  def xmax1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMaxX

  def ymin1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMinY

  def ymax1(i: Int): Double = r1(i).getGeometry.getEnvelopeInternal.getMaxY

  def xmin2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMinX

  def xmax2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMaxX

  def ymin2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMinY

  def ymax2(j: Int): Double = r2(j).getGeometry.getEnvelopeInternal.getMaxY

  /** Tests if the MBRs of two features, one on each list, overlap */
  def rectangleOverlaps(a: Int, b: Int): Boolean =
    r1(a).getGeometry.getEnvelopeInternal.intersects(r2(b).getGeometry.getEnvelopeInternal)

  /**
   * Move to the next matching pair of records. The matching pair (if any), it should always be stored in ii and jj
   *
   * @return whether a result was found `true` or an end-of-list was reached `false`
   */
  def seekToNextOutput(): Boolean = {
    while (i < r1.length && j < r2.length) {
      // 如果没有列表激活，激活包含最左边矩形的列表
      if (activeList == 0) {
        activeList = if (xmin1(i) < xmin2(j)) 1 else 2
        ii = i
        jj = j
      } else if (activeList == 1) {
        jj += 1
      } else if (activeList == 2) {
        ii += 1
      }
      if (activeList == 1) {
        // 修正列表 1 中的记录，并在列表 2 中遍历直到找到第一个匹配的矩形对
        while (jj < r2.length && xmin2(jj) <= xmax1(ii)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj) && referencePointTest(ii, jj)) {
            // 找到一个结果，返回它
            count += 1
            return true
          }
          jj += 1
        }
        // 更新列表 1 中不会产生结果的记录
        do {
          i += 1
        } while (i < r1.length && xmax1(i) < xmin2(j))
        // 重置激活的列表
        activeList = 0
      } else if (activeList == 2) {
        // 修正列表 2 中的记录，并在列表 1 中遍历直到找到第一个匹配的矩形对
        while (ii < r1.length && xmin1(ii) <= xmax2(jj)) {
          if (numMBRTests != null) numMBRTests.add(1)
          if (rectangleOverlaps(ii, jj) && referencePointTest(ii, jj)) {
            // 找到一个结果，返回它
            count += 1
            return true
          }
          ii += 1
        }
        // 更新列表 2 中不会产生结果的记录
        do {
          j += 1
        } while (j < r2.length && xmax2(j) < xmin1(i))
        // 重置激活的列表
        activeList = 0
      }
    }
    // 输出处理的矩形对数量，并返回 false 表示没有找到更多匹配
    logDebug(s"Planesweep processed ${r1.length} X ${r2.length} pairs and found ${count} matches")
    // TODO debug
//    leftCount.add(r1.length)
//    rightCount.add(r2.length)
    false
  }

  /**
   * Run the reference point test between two records #i and #j in the two datasets.
   * Returns `true` if this pair should be reported to the answer. A pair is reported in three cases:
   *
   *  - If its reference point, i.e., top-left corner of the intersection, falls in the duplicate avoidance MBR
   *  - If the intersection MBR has a width of zero and its right-most edge is coincident
   *    with the right-most edge of the duplicate avoidance MBR.
   *  - If the intersection MBR has a height of zero and its top-most edge is coincident
   *    with the top-most edge of the duplicate avoidance MBR
   *
   * The last two conditions are added to handle cases of vertical lines, horizontal lines, or points that
   * define the boundary of a partition. For example, think of the right-most point of a partition that
   * does not technically fall inside the partition but does not belong to any other partitions either.
   *
   * @param i1 the index of the first record
   * @param i2 the index of the second record
   * @return `true` if this pair should be reported in the answer
   */

  private def referencePointTest(i1: Int, i2: Int): Boolean = {
    // 如果不需要避免重复测试，则直接返回 true
    if (dupAvoidanceMBR == null)
      return true

    // 如果有记录测试数量的计数器，则增加计数
    if (numMBRTests != null) numMBRTests.add(1)

    // 计算参考点的 X 和 Y 坐标范围
    val refPointX1: Double = xmin1(i1) max xmin2(i2)
    val refPointX2: Double = xmax1(i1) min xmax2(i2)
    val refPointY1: Double = ymin1(i1) max ymin2(i2)
    val refPointY2: Double = ymax1(i1) min ymax2(i2)

    // 进行避免重复测试

    // 如果参考点的 X 范围在避免重复测试矩形的范围之外，则返回 false
    if (refPointX1 < dupAvoidanceMBR.getMinCoord(0))
      return false
    if (refPointX1 > dupAvoidanceMBR.getMaxCoord(0))
      return false

    // 如果参考点的 X 范围与避免重复测试矩形的右边界相等且参考点的右边界超过左边界，则返回 false
    if (refPointX1 == dupAvoidanceMBR.getMaxCoord(0) && refPointX2 > refPointX1)
      return false

    // 如果参考点的 Y 范围在避免重复测试矩形的范围之外，则返回 false
    if (refPointY1 < dupAvoidanceMBR.getMinCoord(1))
      return false
    if (refPointY1 > dupAvoidanceMBR.getMaxCoord(1))
      return false

    // 如果参考点的 Y 范围与避免重复测试矩形的上边界相等且参考点的上边界超过下边界，则返回 false
    if (refPointY1 == dupAvoidanceMBR.getMaxCoord(1) && refPointY2 > refPointY1)
      return false

    // 如果所有先前的测试都失败，则应报告此点
    true
  }

  private def referencePointTest(f1: IFeature, f2: IFeature, dupAvoidanceMBR: EnvelopeNDLite): Boolean = {
    // 如果不需要避免重复测试，则直接返回 true
    if (dupAvoidanceMBR == null)
      return true

    // 如果有记录测试数量的计数器，则增加计数
    if (numMBRTests != null) numMBRTests.add(1)

    // 计算参考点的 X 和 Y 坐标范围
    val refPointX1: Double = f1.getGeometry.getEnvelopeInternal.getMinX max f2.getGeometry.getEnvelopeInternal.getMinX
    val refPointX2: Double = f1.getGeometry.getEnvelopeInternal.getMaxX min f2.getGeometry.getEnvelopeInternal.getMaxX
    val refPointY1: Double = f1.getGeometry.getEnvelopeInternal.getMinY max f2.getGeometry.getEnvelopeInternal.getMinY
    val refPointY2: Double = f1.getGeometry.getEnvelopeInternal.getMaxX min f2.getGeometry.getEnvelopeInternal.getMaxX

    // 进行避免重复测试

    // 如果参考点的 X 范围在避免重复测试矩形的范围之外，则返回 false
    if (refPointX1 < dupAvoidanceMBR.getMinCoord(0))
      return false
    if (refPointX1 > dupAvoidanceMBR.getMaxCoord(0))
      return false

    // 如果参考点的 X 范围与避免重复测试矩形的右边界相等且参考点的右边界超过左边界，则返回 false
    if (refPointX1 == dupAvoidanceMBR.getMaxCoord(0) && refPointX2 > refPointX1)
      return false

    // 如果参考点的 Y 范围在避免重复测试矩形的范围之外，则返回 false
    if (refPointY1 < dupAvoidanceMBR.getMinCoord(1))
      return false
    if (refPointY1 > dupAvoidanceMBR.getMaxCoord(1))
      return false

    // 如果参考点的 Y 范围与避免重复测试矩形的上边界相等且参考点的上边界超过下边界，则返回 false
    if (refPointY1 == dupAvoidanceMBR.getMaxCoord(1) && refPointY2 > refPointY1)
      return false

    // 如果所有先前的测试都失败，则应报告此点
    true
  }

  override def hasNext: Boolean = i < r1.size && j < r2.size

  override def next(): (T1, T2) = {
    val matchedPair = (r1(ii), r2(jj))
    seekToNextOutput()
    matchedPair
  }
}
