package cn.edu.whu.lynn.davinci

import cn.edu.whu.lynn.davinci.SVGPlotter
import cn.edu.whu.lynn.geolite.{EmptyGeometry, Feature}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class VectorPlotterTest extends FunSuite with ScalaSparkTest {

  test("Extract feature label") {
    val plotter = new SVGPlotter
    // Try without attribute names
    plotter.svgTitle = "simple"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, null, null, Array())) == "simple")
    // Try with attribute names
    plotter.svgTitle = "record #${id}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id"), null, Array(15))) == "record #15")
    plotter.svgTitle = "the ${name-full}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("name-full"), null, Array("Santo"))) == "the Santo")
    // Try with attribute numbers
    plotter.svgTitle = "record #${#0}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id"), null, Array(15))) == "record #15")
    plotter.svgTitle = "the ${#1}"
    assert(plotter.getTitle(Feature.create(EmptyGeometry.instance, Array("id", "name-full"), null, Array(5, "Santo"))) == "the Santo")
  }
}
