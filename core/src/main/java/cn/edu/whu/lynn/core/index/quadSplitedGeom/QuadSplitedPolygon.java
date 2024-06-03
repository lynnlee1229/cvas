package cn.edu.whu.lynn.core.index.quadSplitedGeom;

import cn.edu.whu.lynn.core.index.STRTreeGeometryIndex;
import cn.edu.whu.lynn.core.index.GeometryIndex;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class QuadSplitedPolygon extends Polygon implements Serializable, QuadSplitedGeom {
    GeometryIndex<Geometry> quadIndex = new STRTreeGeometryIndex<>();

    public QuadSplitedPolygon(LinearRing shell, LinearRing[] holes, GeometryFactory factory, List<Geometry> geometries) {
        super(shell, holes, factory);
        for (Geometry geometry : geometries) {
            quadIndex.insert(geometry);
        }
//        System.out.println("索引多边形的构造方法被调用！");
    }

    public Polygon getOriginal() {
        return new Polygon(shell, holes, factory);
    }

    public static QuadSplitedPolygon fromGeom(Polygon polygon, List<Geometry> geometries) {
        LinearRing shell = (LinearRing) polygon.getExteriorRing();
        LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = (LinearRing) polygon.getInteriorRingN(i);
        }
        return new QuadSplitedPolygon(shell, holes, polygon.getFactory(), geometries);
    }

    //override spatial relationship methods
    @Override
    public boolean intersects(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::intersects);
    }

    @Override
    public boolean contains(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::contains);
    }

    @Override
    public boolean within(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::contains);
    }

    @Override
    public boolean overlaps(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::overlaps);
    }

    @Override
    public boolean covers(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().allMatch(g::covers);
    }

    @Override
    public boolean touches(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::touches);
    }

    @Override
    public boolean crosses(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().anyMatch(g::crosses);
    }

    @Override
    public boolean disjoint(Geometry g) {
        return quadIndex.query(g.getEnvelopeInternal()).stream().allMatch(g::disjoint);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof QuadSplitedPolygon) {
            QuadSplitedPolygon otherPolygon = (QuadSplitedPolygon) other;
            return super.equals(other) && quadIndex.equals(otherPolygon.quadIndex);
        }
        return false;
    }
}
