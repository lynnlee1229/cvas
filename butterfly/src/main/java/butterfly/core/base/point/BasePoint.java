package butterfly.core.base.point;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author Lynn Lee
 * @date 2022/9/7
 **/

public class BasePoint extends Point {
  private static final int SRID = 4326;
  public BasePoint(CoordinateSequence coordinates, GeometryFactory factory) {
    super(coordinates, factory);
  }

  public BasePoint(double lng, double lat) {
    super(new CoordinateArraySequence(new Coordinate[] {new Coordinate(lng, lat)}),
        new GeometryFactory(new PrecisionModel(), 4326));
  }

  public void setSRID(int srid) {
    super.setSRID(srid);
  }

  public int getSRID() {
    return super.getSRID();
  }

  public double getLng() {
    return this.getX();
  }

  public void setLng(double lng) {
    this.getCoordinate().setX(lng);
  }

  public double getLat() {
    return this.getY();
  }

  public void setLat(double lat) {
    this.getCoordinate().setY(lat);
  }

  public String toString() {
    return "BasePoint [lng=" + this.getLng() + ", lat=" + this.getLat() + "]";
  }
}
