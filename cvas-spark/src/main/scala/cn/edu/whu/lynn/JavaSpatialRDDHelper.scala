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
package cn.edu.whu.lynn

import cn.edu.whu.lynn.common.ButterflyOptions
import cn.edu.whu.lynn.core.Reprojector.TransformationInfo
import cn.edu.whu.lynn.core.{Reprojector, SJAlgorithm, SJPredicate, SpatialJoinAlgorithms, SpatialPartitioner}
import cn.edu.whu.lynn.davinci.{GeometricPlotter, Plotter}
import cn.edu.whu.lynn.geolite.{EnvelopeNDLite, Feature, IFeature}
import cn.edu.whu.lynn.indexing.IndexHelper
import cn.edu.whu.lynn.indexing.IndexHelper.NumPartitions
import cn.edu.whu.lynn.operations.SpatialJoin
import cn.edu.whu.lynn.synopses.Summary
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.util.LongAccumulator
import org.locationtech.jts.geom.Geometry
import org.opengis.referencing.crs.CoordinateReferenceSystem

/**
 * Helper functions for JavaSpatialRDD
 */
object JavaSpatialRDDHelper {

  /**
   * Tells whether a SpatialRDD is partitioned using any spatial partitioner or not
   * @return {@code true} if the RDD is partitioned using any spatial partitioner
   */
  def isSpatiallyPartitioned(rdd: JavaSpatialRDD): Boolean = rdd.rdd.isSpatiallyPartitioned

  /**
   * Save features as a shapefile
   *
   * @param filename the output filename
   */
  def saveAsShapefile(rdd: JavaSpatialRDD, filename: String): Unit = rdd.rdd.saveAsShapefile(filename)

  /**
   * Save features in GeoJSON format
   * @param filename the output filename
   */
  def saveAsGeoJSON(rdd: JavaSpatialRDD, filename: String): Unit = rdd.rdd.saveAsGeoJSON(filename)

  /**
   * Save features to a CSV or text-delimited file. This method should be used only for point features.
   * @param filename the name of the output file
   * @param xColumn the index of the column that contains the x-coordinate in the output file
   * @param yColumn the index of the column that contains the y-coordinate in the output file
   * @param delimiter the delimiter in the output file, comma by default
   * @param header whether to write a header line, true by default
   */
  def saveAsCSVPoints(rdd: JavaSpatialRDD, filename: String, xColumn: Int, yColumn: Int, delimiter: Char,
                      header: Boolean): Unit =
    rdd.rdd.saveAsCSVPoints(filename, xColumn, yColumn, delimiter, header)

  /**
   * Save features to a CSV file where the geometry is encoded in WKT format
   * @param filename the name of the output file
   * @param wktColumn the index of the column that contains the WKT attribute
   * @param delimiter the delimiter between attributes, tab by default
   * @param header whether to write a header line or not, true by default
   */
  def saveAsWKTFile(rdd: JavaSpatialRDD, filename: String, wktColumn: Int, delimiter: Char, header: Boolean): Unit =
    rdd.rdd.saveAsWKTFile(filename, wktColumn, delimiter, header)

  /**
   * Save features in KML format
   * @param filename the name of the output file
   */
  def saveAsKML(rdd: JavaSpatialRDD, filename: String): Unit = rdd.rdd.saveAsKML(filename)

  /**
   * Write this RDD as a spatial file with the given format and additional options
   * @param filename the output file name
   * @param oformat the output file format (short name)
   * @param opts additional user options
   */
  def writeSpatialFile(rdd: JavaSpatialRDD, filename: String, oformat: String, opts: ButterflyOptions): Unit =
    rdd.rdd.writeSpatialFile(filename, oformat, opts)

  /**
   * Write this RDD as a spatial file with the given format and additional options
   * @param filename the output file name
   * @param oformat the output file format (short name)
   */
  def writeSpatialFile(rdd: JavaSpatialRDD, filename: String, oformat: String): Unit =
    rdd.rdd.writeSpatialFile(filename, oformat, new ButterflyOptions())

  def reproject(rdd: JavaSpatialRDD, targetCRS: CoordinateReferenceSystem): JavaSpatialRDD = {
    val transformationInfo: TransformationInfo = core.Reprojector.findTransformationInfo(
      rdd.first().getGeometry.getSRID, targetCRS)
    rdd.map(f => {
      var g = f.getGeometry
      g = core.Reprojector.reprojectGeometry(g, transformationInfo)
      Feature.create(f, g)
    })
  }

  /**
   * Performs a range query
   *
   * @param range the spatial range to search for
   * @return
   */
  def rangeQuery(rdd: JavaSpatialRDD, range: Geometry): JavaSpatialRDD =
    JavaRDD.fromRDD(rdd.rdd.rangeQuery(range))

  /**
   * Performs a range query while counting the number of MBR tests for profiling the performance.
   * @param rdd the RDD that contains the spatial features
   * @param range the query range
   * @param mbrCount (out) an accumulator that counts the number of MBR tests
   * @return a filtered RDD with the features that intersect the given query range
   */
  def rangeQuery(rdd: JavaSpatialRDD, range: Geometry, mbrCount: LongAccumulator): JavaSpatialRDD =
    JavaRDD.fromRDD(rdd.rdd.rangeQuery(range, mbrCount))

  def rangeQuery(partitionedRDD: JavaPartitionedSpatialRDD, range: Geometry): JavaPartitionedSpatialRDD = {
    val mbb = new EnvelopeNDLite()
    mbb.merge(range)
    val spatialPartitioner: SpatialPartitioner = partitionedRDD.partitioner.get.asInstanceOf[SpatialPartitioner]
    val prunedRDD: RDD[(Integer, IFeature)] = new PartitionPruningRDD(partitionedRDD.rdd,
      partitionID => spatialPartitioner.getPartitionMBR(partitionID).intersectsEnvelope(mbb))
    JavaPairRDD.fromRDD(prunedRDD.filter(f => f._2.getGeometry.intersects(range)))
  }

  /**
   * Performs a spatial join between two Spatial RDDs and returns a [[JavaPairRDD]] with matching features
   * @param rdd1 the first RDD to join
   * @param rdd2 the second RDD to join
   * @param predicate the predicate that matches two features from the two inputs
   * @param algorithm the distributed algorithm used to join the two inputs
   * @return a [[JavaPairRDD]] with matching pairs of features.
   */
  def spatialJoin(rdd1: JavaSpatialRDD, rdd2: JavaSpatialRDD,
                  predicate: SJPredicate,
                  algorithm: SJAlgorithm): JavaPairRDD[IFeature, IFeature] =
    SpatialJoin.spatialJoin(rdd1, rdd2, predicate, algorithm, null, new ButterflyOptions())

  /**
   * Performs a spatial join between two Spatial RDDs and returns a [[JavaPairRDD]] with intersecting
   * (non-disjoint) features.
   * @param rdd1 the first RDD to join
   * @param rdd2 the second RDD to join
   */
  def spatialJoin(rdd1: JavaSpatialRDD, rdd2: JavaSpatialRDD): JavaPairRDD[IFeature, IFeature] =
    spatialJoin(rdd1, rdd2, SJPredicate.Intersects,
      SJAlgorithm.PBSM)

  /**
   * Compute the geometric summary of the given RDD
   * @param rdd the spatial RDD to compute its summary
   * @return the summary of the given RDD
   */
  def summary(rdd: JavaSpatialRDD): Summary = rdd.rdd.summary

  /**
   * Partition a set of features according to a created spatial partitioner
   *
   * @param spatialPartitioner the partitioner for the data
   * @return partitioned records
   */
  def spatialPartition(rdd: JavaSpatialRDD, spatialPartitioner: SpatialPartitioner): JavaPartitionedSpatialRDD =
    IndexHelper.partitionFeatures(rdd, spatialPartitioner)

  /**
   * Partitions this RDD using the given partitioner type. If the desired number of partitions is not provided,
   * the output number of partitions will be roughly equal to the number of partitions in the input RDD.
   * @param rdd the input features to partition
   * @param partitionerClass the class of the partitioner
   * @param numPartitions the desired number of partitions. If not set, the number of partitions of the input RDD is used.
   * @return a new RDD that is partitioned using the given partitioner class
   */
  def spatialPartition(rdd: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                       numPartitions: Int): JavaPartitionedSpatialRDD =
    spatialPartition(rdd, partitionerClass, numPartitions, new ButterflyOptions())

  /**
   * Partitions this RDD using the given partitioner type. If the desired number of partitions is not provided,
   * the output number of partitions will be roughly equal to the number of partitions in the input RDD.
   * @param rdd the input features to partition
   * @param partitionerClass the class of the partitioner
   * @param numPartitions the desired number of partitions. If not set, the number of partitions of the input RDD is used.
   * @param opts additional options for initializing the partitioner
   * @return a new RDD that is partitioned using the given partitioner class
   */
  def spatialPartition(rdd: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner],
                       numPartitions: Int, opts: ButterflyOptions): JavaPartitionedSpatialRDD = {
    val partitioner = IndexHelper.createPartitioner(rdd, partitionerClass,
      NumPartitions(IndexHelper.Fixed, numPartitions),
      _ => 1,
      opts
    )
    IndexHelper.partitionFeatures(rdd, partitioner)
  }

  /**
   * Partitions this RDD using the given partitioner type. The output number of partitions will be
   * roughly equal to the number of partitions in the input RDD.
   * @param partitionerClass the class of the partitioner
   * @return a new RDD that is partitioned using the given partitioner class
   */
  def spatialPartition(rdd: JavaSpatialRDD, partitionerClass: Class[_ <: SpatialPartitioner]): JavaPartitionedSpatialRDD =
    spatialPartition(rdd, partitionerClass, rdd.getNumPartitions)

  /**
   * Writes a spatially partitioned RDD as a set of files, one for each partition and adds a _master file that
   * stores information about the partitions
   * @param indexPath the output path to write to
   */
  def saveAsIndex(partitionedRDD: JavaPartitionedSpatialRDD, indexPath: String): Unit =
    IndexHelper.saveIndex(partitionedRDD, indexPath, new ButterflyOptions().set("oformat", "rtree"))

  // ------- Visualization functions
  /**
   * Plots the features to an image using the given plotter
   * @param imageWidth the width of the image in pixels
   * @param imageHeight the height of the image in pixels
   * @param imagePath the path to write the generated image
   * @param plotterClass the plotter class
   * @param opts additional user options
   */
  def plotImage(rdd: JavaSpatialRDD, imageWidth: Int, imageHeight: Int, imagePath: String,
                plotterClass: Class[_ <: Plotter], opts: ButterflyOptions): Unit =
    rdd.rdd.plotImage(imageWidth, imageHeight, imagePath, plotterClass, opts)

  /**
   * Plots the features to an image using the [[GeometricPlotter]]
   * @param imageWidth the width of the image in pixels
   * @param imageHeight the height of the image in pixels
   * @param imagePath the path to write the generated image
   * @param opts additional user options
   */
  def plotImage(rdd: JavaSpatialRDD, imageWidth: Int, imageHeight: Int, imagePath: String, opts: ButterflyOptions): Unit =
    plotImage(rdd, imageWidth, imageHeight, imagePath, classOf[GeometricPlotter], opts)

  /**
   * Plots the features to an image using the [[GeometricPlotter]] with default options
   * @param imageWidth the width of the image in pixels
   * @param imageHeight the height of the image in pixels
   * @param imagePath the path to write the generated image
   */
  def plotImage(rdd: JavaSpatialRDD, imageWidth: Int, imageHeight: Int, imagePath: String): Unit =
    plotImage(rdd, imageWidth, imageHeight, imagePath)

  /**
   * Performs a raster X vector join (Raptor join) between the two given RDDs.
   * @param vectors the set of vector features
   * @param rasters the set of raster tiles
   * @param opts additional options for the algorithm
   * @return the intersection between the feature vectors and all raster pixels.
   */
//  def raptorJoin[T](vectors: JavaSpatialRDD, rasters: JavaRasterRDD[T], opts: BeastOptions):
//    JavaRDD[RaptorJoinFeature[T]] = RaptorJoin.raptorJoinFeatureJ(rasters, vectors, opts)
}
