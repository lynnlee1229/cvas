package cn.edu.whu.lynn.io

import cn.edu.whu.lynn.common.ButterflyOptions
import cn.edu.whu.lynn.geolite.{Feature, GeometryReader, IFeature}
import cn.edu.whu.lynn.io.ReadWriteMixin._
import cn.edu.whu.lynn.io.SpatialWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.CoordinateXY
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.{File, FilenameFilter}

@RunWith(classOf[JUnitRunner])
class SpatialWriterTest extends FunSuite with ScalaSparkTest {

  test("Write GeoJSON compressed file") {
    val outputPath = new File(scratchDir, "test.geojson.bz2")
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(
      Feature.create(null, GeometryReader.DefaultGeometryFactory.createPoint(new CoordinateXY(1, 2)))
    ))
    SpatialWriter.saveFeatures(features, "geojson", outputPath.getPath, new ButterflyOptions())
    // Make sure that the final output file has .bz2 extension
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".bz2")
    })
    assertResult(1)(files.length)
  }

  test("Write non-compressed shapefile") {
    val outputPath = new File(scratchDir, "test.shp")
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(
      Feature.create(null, GeometryReader.DefaultGeometryFactory.createPoint(new CoordinateXY(1, 2)))
    ))
    SpatialWriter.saveFeatures(features, "shapefile", outputPath.getPath, new ButterflyOptions())
    // Make sure that the final output file has .bz2 extension
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.endsWith(".shp") || name.endsWith("shx") || name.endsWith("dbf")
      }
    })
    assertResult(3)(files.length)
  }

  test("Write a SpatialRDD with empty partitions") {
    // SpatialWriter should skip empty partitions instead of writing an empty file there. Issue #4
    val outputPath = new File(scratchDir, "test.csv")
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(
      Feature.create(null, GeometryReader.DefaultGeometryFactory.createPoint(new CoordinateXY(1, 2)))
    ), 4)
    SpatialWriter.saveFeatures(features, "wkt", outputPath.getPath, new ButterflyOptions())
    // Make sure that only the non-empty partitions were written
    val files = outputPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".csv")
    })
    assertResult(1)(files.length)
  }

  test("Write a SpatialRDD with one partition in compatibility mode") {
    // SpatialWriter should skip empty partitions instead of writing an empty file there. Issue #4
    val outputPath = new File(scratchDir, "test.csv")
    val features: RDD[IFeature] = sparkContext.parallelize(Seq(
      Feature.create(null, GeometryReader.DefaultGeometryFactory.createPoint(new CoordinateXY(1, 2)))
    ), 1)
    SpatialWriter.saveFeatures(features, "wkt", outputPath.getPath, "compatibility" -> true)
    // Verify that no output directory was created and that only one file was created.
    assert(outputPath.isFile, "Output path should point to a file not directory")
  }
}
