/*
 * Copyright 2020 University of California, Riverside
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
package cn.edu.whu.lynn.synopses

import cn.edu.whu.lynn
import cn.edu.whu.lynn.core.CGOperationsMixin._
import cn.edu.whu.lynn.core.SpatialDataTypes.{JavaSpatialRDD, SpatialRDD}
import cn.edu.whu.lynn.geolite.{EnvelopeNDLite, IFeature, PointND}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.varargs

/**
 * A helper object to calculate histograms for features.
 */
object HistogramOP extends Logging {
  /**A choice of a computation method for histograms {[[OnePass]], [[OneHalfPass]], [[TwoPass]]}*/
  trait ComputationMethod
  // 一次遍历
  case object OnePass extends ComputationMethod
  // 一次半遍历
  case object OneHalfPass extends ComputationMethod
  // 两次遍历
  case object TwoPass extends ComputationMethod
  // 稀疏
  case object Sparse extends ComputationMethod

  /**A choice of a histogram type, {[[PointHistogram]], [[EulerHistogram]]}*/
  trait HistogramType
  case object PointHistogram extends HistogramType
  case object EulerHistogram extends HistogramType

  /**
   * Compute the grid dimension from the given sequence of numbers. If the given sequence contains a single number,
   * it is treated as a total number of partitions and the number of partitions along each dimension is computed
   * using the function [[UniformHistogram#computeNumPartitions(Envelope, int)]]
   *
   * @param mbr the MBR of the input space. Used to compute square-like cells
   * @param numPartitions the desired number of partitions. Either a single number of a sequence of dimensions
   * @return an array of number of partitions along each axis
   */
  @varargs def computeGridDimensions(mbr: EnvelopeNDLite, numPartitions: Int*): Array[Int] = {
    if (numPartitions.size < mbr.getCoordinateDimension) {
      // Treat it as number of buckets and compute the number of partitions correctly
      val numBuckets: Int = numPartitions.product
      lynn.synopses.UniformHistogram.computeNumPartitions(mbr, numBuckets)
    } else {
      numPartitions.toArray
    }
  }

  /**
   * 计算稀疏点直方图
   *
   * @param features    空间RDD，包含要进行直方图计算的特征
   * @param sizeFunction 用于计算特征大小的函数
   * @param mbb          EnvelopeNDLite，表示计算直方图的空间范围
   * @param numBuckets   用于直方图计算的桶的数量，可变参数
   * @return UniformHistogram，表示计算得到的直方图
   */
  @varargs
  def computePointHistogramSparse(features: SpatialRDD, sizeFunction: IFeature => Int,
                                  mbb: EnvelopeNDLite, numBuckets: Int*): UniformHistogram = {
    // 计算网格的维度
    val gridDimensions: Array[Int] = computeGridDimensions(mbb, numBuckets: _*)

    // 计算每个特征所属的网格ID以及对应的大小
    val binSize: RDD[(Int, Long)] = features.map(feature => {
      val center = new PointND(feature.getGeometry)
      val binID = UniformHistogram.getPointBinID(center, mbb, gridDimensions)
      (binID, sizeFunction(feature).toLong)
    }).filter(_._1 >= 0)

    // 按网格ID进行累加，得到最终的大小
    val finalSizes: RDD[(Int, Long)] = binSize.reduceByKey(_+_)

    // 创建最终的UniformHistogram对象
    val finalHistogram: UniformHistogram = new UniformHistogram(mbb, gridDimensions:_*)

    // 将计算得到的大小填充到最终的直方图中
    finalSizes.collect.foreach(pt => finalHistogram.values(pt._1) = pt._2)

    // 返回最终的直方图
    finalHistogram
  }

  /**
   * 通过两次遍历计算点直方图，将每个特征分配到包含其质心的网格单元格中
   *
   * @param features     特征的集合
   * @param sizeFunction 一个将每个特征映射到大小的函数
   * @param mbb          输入数据的最小边界框或计算直方图的区域
   * @param numBuckets   每个维度的分区数。如果只提供一个值，它将被视为总桶数的提示。
   * @return 计算得到的直方图
   */
  @varargs
  def computePointHistogramTwoPass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                   mbb: EnvelopeNDLite, numBuckets: Int*): UniformHistogram = {
    // 计算网格的维度
    val gridDimensions: Array[Int] = computeGridDimensions(mbb, numBuckets: _*)

    // 初始化局部直方图集合
    var partialHistograms: RDD[UniformHistogram] = features.mapPartitions(features => {
      val partialHistogram: UniformHistogram = new UniformHistogram(mbb, gridDimensions:_*)
      while (features.hasNext) {
        val feature = features.next()
        // 如果特征的几何信息不为空，则将其贡献到局部直方图中
        if (!feature.getGeometry.isEmpty) {
          val point = new PointND(feature.getGeometry)
          partialHistogram.addPoint(point, sizeFunction(feature))
        }
      }
      Option(partialHistogram).iterator
    })

    // 计算可以一起减少的直方图数量
    val size = numBuckets.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf
      .getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200

    // 如果局部直方图数量超过最大减少数量，则进行coalesce以减少分区数
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms = partialHistograms.coalesce(maxReduce)

    // 最终的直方图减少操作，通过合并对齐的直方图
    partialHistograms.reduce((h1, h2) => h1.mergeAligned(h2))
  }

  @varargs def computePointHistogramTwoPass(features: JavaSpatialRDD,
                                            sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                            mbb: EnvelopeNDLite, numBuckets: Int*) : UniformHistogram =
    computePointHistogramTwoPass(features.rdd, f => sizeFunction.call(f), mbb, numBuckets:_*)

  /**
   * Computes a point histogram which assigns each feature to the cell that contains its centroid
   * @param features the set of features
   * @param sizeFunction a function that maps each feature to a size
   * @param numBuckets the number of partitions per dimension. If only one value is provided, it is treated as
   *                      a hint for the total number of buckets.
   * @return the computed histogram
   */
  @varargs def computePointHistogramOnePass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                            numBuckets: Int*) : UniformHistogram = {
    val partialHistograms: RDD[UniformHistogram] = computePartialHistograms(features, sizeFunction, numBuckets:_*)
    // Merge partial histograms into one
    partialHistograms.reduce((ph1, ph2) => {
      if (ph1.containsEnvelope(ph2)) {
        ph1.mergeNonAligned(ph2)
      } else {
        val combinedMBR = new EnvelopeNDLite(ph1)
        combinedMBR.merge(ph2)
        val finalHistogram = new UniformHistogram(combinedMBR, computeGridDimensions(combinedMBR, numBuckets:_*):_*)
        finalHistogram.mergeNonAligned(ph1).mergeNonAligned(ph2)
      }
    })
  }

  /**
   * Computes a point histogram which assigns each feature to the cell that contains its centroid
   * @param features the set of features
   * @param sizeFunction a function that maps each feature to a size
   * @param numBuckets the number of partitions per dimension. If only one value is provided, it is treated as
   *                      a hint for the total number of buckets.
   * @return the computed histogram
   */
  @varargs def computePointHistogramOneHalfPass(features: SpatialRDD, sizeFunction: IFeature => Int,
                                                numBuckets: Int*) : UniformHistogram = {
    val partialHistograms: RDD[UniformHistogram] = computePartialHistograms(features, sizeFunction, numBuckets:_*)
    // Cache all partial histogram
    partialHistograms.persist(StorageLevel.MEMORY_AND_DISK)
    // Compute the MBR of all histogram
    val mbrCombineFunc = (mbr1: EnvelopeNDLite, mbr2: EnvelopeNDLite) => mbr1.merge(mbr2)
    val combinedMBR = partialHistograms.aggregate(new EnvelopeNDLite())(mbrCombineFunc, mbrCombineFunc)
    // Merge all the partial histograms into the final histogram
    val histogramCombineFunc = (hist1: UniformHistogram, hist2: UniformHistogram) => hist1.mergeNonAligned(hist2)
    val finalHistogram = new UniformHistogram(combinedMBR, computeGridDimensions(combinedMBR, numBuckets:_*):_*)
    partialHistograms.aggregate(finalHistogram)(histogramCombineFunc, histogramCombineFunc)
  }

  /**
   * An internal function to compute partial histograms for each partition
   * @param features the set of features
   * @param sizeFunction the function that calculates the size for each features
   * @param numBuckets the number of partitions per histogram
   * @return the computed partial histograms
   */
  private def computePartialHistograms(features: SpatialRDD, sizeFunction: IFeature => Int,
                                       numBuckets: Int*): RDD[UniformHistogram] = {
    val partialHistograms: RDD[UniformHistogram] = features.mapPartitions(features => {
      val pointSizesCached: Array[(PointND, Int)] = features.map(f => (new PointND(f.getGeometry), sizeFunction(f))).toArray
      val mbb = new EnvelopeNDLite(pointSizesCached(0)._1.getCoordinateDimension)
      mbb.setEmpty()
      pointSizesCached.foreach(pointSize => mbb.merge(pointSize._1))
      val partialHistogram = new UniformHistogram(mbb, computeGridDimensions(mbb, numBuckets: _*): _*)
      pointSizesCached.foreach(pointSize => partialHistogram.addPoint(pointSize._1, pointSize._2))
      Some(partialHistogram).iterator
    })
    // Calculate how many histograms we can reduce together
    val size = numBuckets.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf
      .getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms.coalesce(maxReduce)
    else
      partialHistograms
  }

  /**
   * Compute the Euler histogram of the given set of features. The resulting histogram contains four
   * values per cell (in two dimensions). Currently, this method only works for two dimensions
   * @param dataRecords the data points contains pairs of envelopes and values
   * @param inputMBB the MBR of the input space
   * @param numPartitions the number of partitions to create in the histogram
   */
  @varargs def computeEulerHistogram(dataRecords: SpatialRDD, sizeFunction: IFeature => Int,
                                     inputMBB: EnvelopeNDLite, numPartitions : Int*): EulerHistogram2D = {
    val gridDimensions = computeGridDimensions(inputMBB, numPartitions:_*)
    var partialHistograms = dataRecords.mapPartitions(features => {
      val partialHistogram = new EulerHistogram2D(inputMBB, gridDimensions(0), gridDimensions(1))
      while (features.hasNext) {
        val feature = features.next
        // Make sure the MBR fits within the boundaries
        val featureMBB: EnvelopeNDLite = new EnvelopeNDLite().merge(feature.getGeometry)
        featureMBB.shrink(inputMBB)
        if (!featureMBB.isEmpty)
          partialHistogram.addEnvelope(featureMBB.getMinCoord(0), featureMBB.getMinCoord(1),
            featureMBB.getMaxCoord(0), featureMBB.getMaxCoord(1), sizeFunction(feature))
      }
      Some(partialHistogram).iterator
    })
    // Calculate how many histograms we can reduce together
    val size = numPartitions.product * 8 + 100
    val maxReduce: Int = (partialHistograms.sparkContext.getConf.getSizeAsBytes("spark.driver.maxResultSize", "1g") / size).toInt max 200
    if (partialHistograms.getNumPartitions > maxReduce)
      partialHistograms = partialHistograms.coalesce(maxReduce)
    partialHistograms.reduce((h1, h2) => h1.mergeAligned(h2))
  }

  /**
   * (Java Shortcut to)
   * Compute the Euler histogram of the given set of features. The resulting histogram contains four
   * values per cell (in two dimensions). Currently, this method only works for two dimensions
   * @param dataRecords the data points contains pairs of envelopes and values
   * @param sizeFunction the function that computes the size of a feature
   * @param inputMBB the MBR of the input space
   * @param numPartitions the number of partitions to create in the histogram
   * @return the histogram of the input data
   * Note: We rely on [[org.apache.spark.api.java.function.Function]]
   * rather than [[scala.Function1]] to allow lambda expressions from Java without running into serialization issues
   */
  @varargs def computeEulerHistogram(dataRecords: JavaSpatialRDD,
                                     sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                     inputMBB: EnvelopeNDLite, numPartitions : Int*): EulerHistogram2D =
    computeEulerHistogram(dataRecords.rdd, f => sizeFunction.call(f), inputMBB, numPartitions:_*)

  /**
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param sizeFunction the function that computes the size of each feature
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, sizeFunction: IFeature => Int,
                                method: ComputationMethod, htype: HistogramType, numBuckets: Int*): AbstractHistogram =
    (method, htype) match {
      case (Sparse, PointHistogram) => computePointHistogramSparse(features, sizeFunction, features.summary, numBuckets:_*)
      case (TwoPass, PointHistogram) => computePointHistogramTwoPass(features, sizeFunction, features.summary, numBuckets:_*)
      case (OneHalfPass, PointHistogram) => computePointHistogramOneHalfPass(features, sizeFunction, numBuckets:_*)
      case (OnePass, PointHistogram) => computePointHistogramOnePass(features, sizeFunction, numBuckets:_*)
      case (TwoPass, EulerHistogram) => computeEulerHistogram(features, sizeFunction, features.summary, numBuckets:_*)
      case other => throw new RuntimeException(s"Unsupported histogram computation method '$other'")
    }

  /**
   * (Java Shortcut to)
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param sizeFunction the function that computes the size of each feature
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD,
                                sizeFunction: org.apache.spark.api.java.function.Function[IFeature, Int],
                                method: ComputationMethod, htype: HistogramType, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, f => sizeFunction.call(f), method, htype, numBuckets:_*)

  /**
   * Compute a count histogram for the given set of features where each bucket contains the number of features
   * @param features the features to compute their histogram
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, method: ComputationMethod,
                                numBuckets: Int*): AbstractHistogram =
    computeHistogram(features, _=>1, method, PointHistogram, numBuckets:_*)

  /**
   * (Java Shortuct to)
   * Compute a count histogram for the given set of features where each bucket contains the number of features
   * @param features the features to compute their histogram
   * @param method the computation method {TwoPass, OneHalfPass, OnePass}
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD, method: ComputationMethod,
                                numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, method, numBuckets:_*)


  /**
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: SpatialRDD, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features, TwoPass, numBuckets:_*)

  /**
   * (Java Shortcut to)
   * Compute a histogram for the given set of features according to the given parameters
   * @param features the features to compute their histogram
   * @param numBuckets the desired number of buckets in the generated histogram
   * @return the computed histogram
   */
  @varargs def computeHistogram(features: JavaSpatialRDD, numBuckets: Int*): AbstractHistogram =
    computeHistogram(features.rdd, numBuckets:_*)
}
