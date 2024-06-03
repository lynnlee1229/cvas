package cn.edu.whu.lynn.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.quadtree.Quadtree;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class QuadTreeGeometryIndex<T extends Geometry> implements GeometryIndex<T> {
    private final Quadtree quadTree;

    public QuadTreeGeometryIndex() {
        this.quadTree = new Quadtree();
    }

    @Override
    public void insert(List<T> geometries) {
        geometries.forEach(this::insert);
    }

    @Override
    public void insert(Geometry geom) {
        quadTree.insert(geom.getEnvelopeInternal(), geom);
    }

    @Override
    public List<T> query(Envelope envelope) {
        return quadTree.query(envelope);
    }

    @Override
    public List<T> query(Geometry geometry) {
        return query(geometry.getEnvelopeInternal());
    }

    @Override
    public List query(Geometry geom, double distance) {
        return null;
    }

    @Override
    public void remove(Geometry geom) {
        quadTree.remove(geom.getEnvelopeInternal(), geom);
    }

    @Override
    public int size() {
        return quadTree.size();
    }
}
