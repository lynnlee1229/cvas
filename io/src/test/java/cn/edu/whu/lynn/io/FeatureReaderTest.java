package cn.edu.whu.lynn.io;

import cn.edu.whu.lynn.test.JavaSpatialSparkTest;

public class FeatureReaderTest extends JavaSpatialSparkTest {

  public void testGetFileExtension() {
    String expectedExtension = ".geojson";
    String actualExtension = FeatureReader.getFileExtension("geojson");
    assertEquals(expectedExtension, actualExtension);
  }

}