package cn.edu.whu.lynn.geolite;

import cn.edu.whu.lynn.geolite.PointND;
import junit.framework.TestCase;
import org.locationtech.jts.geom.GeometryFactory;

public class PointNDTest extends TestCase {

  public void testSetEmptyWithZeroCoordinates() {
    GeometryFactory geometryFactory = new GeometryFactory();
    PointND p = new PointND(geometryFactory);
    p.setCoordinateDimension(0);
    p.setEmpty();

    p = new PointND(geometryFactory, 0);
    p.setEmpty();
  }
}