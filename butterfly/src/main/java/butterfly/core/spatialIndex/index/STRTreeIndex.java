package butterfly.core.spatialIndex.index;

import butterfly.core.utils.distance.DistanceCalculator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/21
 **/
public class STRTreeIndex<T extends Geometry> extends SpatialIndexBase<T> {
    private static final int DEFAULT_NODE_CAPACITY = 2;
    private final STRtree stRtree;

    public STRTreeIndex() {
        stRtree = new STRtree(DEFAULT_NODE_CAPACITY);
    }

    public STRTreeIndex(int nodeCapacity) {
        stRtree = new STRtree(nodeCapacity);
    }

    @Override
    public void insert(List<T> geometries) {
        geometries.forEach(this::insert);
    }

    @Override
    public void insert(Geometry geom) {
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
    public List<T> query(Geometry geom, double distance, DistanceCalculator calculator) {
        Point point = geom instanceof Point ? (Point) geom : geom.getCentroid();
        Envelope envelope = calculator.calcBoxByDist(point, distance);
        List<T> result = stRtree.query(envelope);
        result.removeIf(geometry -> calculator.calcDistance(point, geometry) > distance);
        return result;
    }

    @Override
    public void remove(Geometry geom) {
        stRtree.remove(geom.getEnvelopeInternal(), geom);
    }

    @Override
    public int size() {
        return stRtree.size();
    }
}
