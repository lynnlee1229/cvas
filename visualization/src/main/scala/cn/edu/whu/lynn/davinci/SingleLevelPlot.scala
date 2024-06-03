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
package cn.edu.whu.lynn.davinci

import cn.edu.whu.lynn.common.{ButterflyOptions, CLIOperation}
import cn.edu.whu.lynn.core.SpatialDataTypes
import cn.edu.whu.lynn.geolite.EnvelopeNDLite
import cn.edu.whu.lynn.synopses.Summary
import cn.edu.whu.lynn.util.OperationMetadata
import SpatialDataTypes.SpatialRDD
import cn.edu.whu.lynn.davinci.{Canvas, CommonVisualizationHelper, GeometricPlotter, Plotter, SingleLevelPlotHelper}
import cn.edu.whu.lynn.io.{SpatialFileRDD, SpatialOutputFormat}
import cn.edu.whu.lynn.io.ReadWriteMixin._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

@OperationMetadata(
  shortName =  "splot",
  description = "Plots the input file as a single image",
  inputArity = "1",
  outputArity = "1",
  inheritParams = Array(classOf[SpatialFileRDD], classOf[SpatialOutputFormat],
    classOf[CommonVisualizationHelper], classOf[SingleLevelPlotHelper])
)
object SingleLevelPlot extends CLIOperation with Logging {

  /**
   * Plots a set of features to a single image. By default, the aspect ratio of the input is maintained and
   * the given dimensions are treated as upper bounds for image width and height, i.e., the produced image
   * might have smaller dimensions. Also, by default, the extents of the canvas will be equal to the input data.
   * This means that the plotted image will occupy the largest portion of the image. If you wish to visualize
   * only a subset of the data or visualize the data on a small portion of the image, you can specify the [[canvasMBR]]
   * parameter.
   * @param features the set of features to plot
   * @param imageWidth the width of the image in pixels
   * @param imageHeight the height of the image in pixels.
   * @param imagePath the path to which the image will be written
   * @param plotterClass the class of the plotter to use for producing the image
   * @param canvasMBR (Optional) the extents of the data (minimum bounding rectangle)
   * @param opts (Optional) additional options to use with the plotter, e.g., colors
   */
  def plotFeatures(features: SpatialDataTypes.SpatialRDD, imageWidth: Int, imageHeight: Int, imagePath: String,
                   plotterClass: Class[_ <: Plotter] = classOf[GeometricPlotter], canvasMBR: EnvelopeNDLite = null,
                   opts: ButterflyOptions = new ButterflyOptions()): Unit = {
    val summary: EnvelopeNDLite = if (canvasMBR != null) canvasMBR else Summary.computeForFeatures(features)
    // Adjust image size to maintain aspect ratio if desired
    var width = imageWidth
    var height = imageHeight
    if (opts.getBoolean(CommonVisualizationHelper.KeepRatio, true)) {
      val adjustedWidth: Int = (imageHeight * summary.getSideLength(0) / summary.getSideLength(1)).toInt
      val adjustedHeight: Int = (imageWidth * summary.getSideLength(1) / summary.getSideLength(0)).toInt
      width = Math.min(imageWidth, adjustedWidth)
      height = Math.min(imageHeight, adjustedHeight)
    }
    val partialCanvases: RDD[Canvas] = features.mapPartitions(iFeatureIterator => {
      val plotter: Plotter = plotterClass.getConstructor().newInstance()
      plotter.setup(opts)
      val canvas: Canvas = plotter.createCanvas(width, height, summary.toJTSEnvelope, 0)
      for (feature <- iFeatureIterator)
        plotter.plot(canvas, feature)
      Option(canvas).iterator
    })

    // Merge the partial canvases into one final canvas
    val plotter: Plotter = plotterClass.getConstructor().newInstance()
    plotter.setup(opts)
    val finalCanvas: Canvas = partialCanvases.reduce((c1, c2) => plotter.merge(c1, c2))

    val outPath: Path = new Path(imagePath)
    val outFileSystem: FileSystem = outPath.getFileSystem(opts.loadIntoHadoopConf(features.context.hadoopConfiguration))
    val outStream = outFileSystem.create(outPath)
    plotter.writeImage(finalCanvas, outStream, opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true))
    outStream.close()
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options for configuring the operation
    * @param sc   the Spark context used to run the operation
    * @return an optional result of this operation
    */
  override def run(opts: ButterflyOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val features: SpatialRDD = sc.spatialFile(inputs(0), opts)
    val plotterName: String = opts.getString(CommonVisualizationHelper.PlotterName)
    val plotterClass: Class[_ <: Plotter] = Plotter.getPlotterClass(plotterName)
    val imageWidth: Int = opts.getInt(SingleLevelPlotHelper.ImageWidth, 1000)
    val imageHeight: Int = opts.getInt(SingleLevelPlotHelper.ImageHeight, 1000)
    import VisualizationMixin._
    features.plotImage(imageWidth, imageHeight, outputs(0), plotterClass, opts)
  }
}
