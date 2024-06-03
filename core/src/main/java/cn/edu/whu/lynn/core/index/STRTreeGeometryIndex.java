package cn.edu.whu.lynn.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/3/27
 **/
public class STRTreeGeometryIndex<T extends Geometry> implements GeometryIndex<T>, Serializable {

  private static final int DEFAULT_NODE_CAPACITY = 2;
  private final STRtree stRtree;

  public STRTreeGeometryIndex() {
    stRtree = new STRtree(DEFAULT_NODE_CAPACITY);
  }

  public STRTreeGeometryIndex(int nodeCapacity) {
    stRtree = new STRtree(nodeCapacity);
  }

  @Override
  public void insert(List<T> geometries) {
    geometries.forEach(this::insert);
  }

  @Override
  public void insert(T geom) {
    stRtree.insert(geom.getEnvelopeInternal(), geom);
  }

  @Override
  public List<T> query(Envelope envelope) {
    return stRtree.query(envelope);
  }

  @Override
  public List<T> query(Geometry geometry) {
    return query(geometry.getEnvelopeInternal());
  }

  @Override
  public List<T> query(Geometry geometry, double distance) {
    return null;
//    Point point = geometry instanceof Point ? (Point) geometry : geometry.getCentroid();
//    Envelope envelope = getEnvelopeByDis(point, distance);
//    List<T> result = stRtree.query(envelope);
//    result.removeIf(geom -> getEuclideanDistance(point, geom, "km") > distance);
//    return result;
  }

  /**
   * STRTree use `==` to just the equality of objects int the tree,
   * so only support for removing object with the same address.
   */
  @Override
  public void remove(T geom) {
    stRtree.remove(geom.getEnvelopeInternal(), geom);
  }

  @Override
  public int size() {
    return stRtree.size();
  }

  @Override
  public String toString() {
    return stRtree.toString();
  }
}
