package butterfly.core.spatialIndex.index;

import butterfly.core.utils.distance.DistanceCalculator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Objects;

/**
 * @author Lynn Lee
 * @date 2024/1/21
 **/
public interface IGeomSpatialIndex<T extends Geometry> {

    class Node<T extends Geometry> {
        T geom;
        Object attr;

        public Node(T geom) {
            this.geom = geom;
            this.attr = geom.getUserData();
        }

        public T getGeom() {
            return geom;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node<?> node = (Node<?>) o;
            return Objects.equals(attr, node.attr);
        }

        @Override
        public int hashCode() {
            return attr.hashCode();
        }

        @Override
        public String toString() {
            return "Node{geom=" + geom + ", attr=" + attr + '}';
        }
    }

    /**
     * Insert a list of geometries into the index.
     *
     * @param geometries the geometries to be inserted.
     */
    void insert(List<T> geometries);

    /**
     * Insert a geometry into the index.
     *
     * @param geom the geometry to be inserted.
     */
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
    List<T> query(Geometry geometry);

    /**
     * Query with the given distance. If the geometry is not a point,
     * the distance is the distance from the center of the geometry.
     *
     * @param geom     the query geometry
     * @param distance the query distance
     * @return all geometries in the tree which within the distance will be return.
     */
    List<T> query(Geometry geom, double distance, DistanceCalculator calculator);

    void remove(T geom);

    int size();
}
