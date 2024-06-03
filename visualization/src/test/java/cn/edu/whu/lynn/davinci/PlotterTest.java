package cn.edu.whu.lynn.davinci;

import cn.edu.whu.lynn.davinci.GeometricPlotter;
import cn.edu.whu.lynn.davinci.Plotter;
import cn.edu.whu.lynn.test.JavaSparkTest;

public class PlotterTest extends JavaSparkTest {

  public void testGetExtension() {
    String expectedExtension = ".png";
    String actualExtension = Plotter.getImageExtension("gplot");
    assertEquals(expectedExtension, actualExtension);
  }

  public void testGetExtensionFromClass() {
    String expectedExtension = ".png";
    String actualExtension = Plotter.getImageExtension(GeometricPlotter.class);
    assertEquals(expectedExtension, actualExtension);
  }

}