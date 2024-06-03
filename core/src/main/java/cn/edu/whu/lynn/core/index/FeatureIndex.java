package cn.edu.whu.lynn.core.index;

import cn.edu.whu.lynn.geolite.IFeature;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/3/27
 **/
public interface FeatureIndex<T extends IFeature> extends Serializable {

  void insert(List<T> geometries);

  void insert(T geom);

  /**
   * Query with a bounding box.
   *
   * @param envelope query bounding box.
   * @return all geometries in the tree which intersects with the query envelope.
   */
  List<T> query(Envelope envelope);

  /**
   * Query with a geometry.
   *
   * @param geometry query geometry
   * @return all geometries in the tree which intersects with the query geometry.
   */
  List<T> query(IFeature geometry);

  void remove(T geom);

  int size();
}
