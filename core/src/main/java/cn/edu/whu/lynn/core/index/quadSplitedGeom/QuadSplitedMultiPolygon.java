package cn.edu.whu.lynn.core.index.quadSplitedGeom;

import cn.edu.whu.lynn.core.index.STRTreeGeometryIndex;
import cn.edu.whu.lynn.core.index.GeometryIndex;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class QuadSplitedMultiPolygon extends MultiPolygon implements Serializable, QuadSplitedGeom{
    GeometryIndex<Geometry> quadIndex = new STRTreeGeometryIndex<>();

    public QuadSplitedMultiPolygon(Polygon[] polygons, GeometryFactory factory, List<Geometry> geometries) {
        super(polygons, factory);
        for (Geometry geometry : geometries) {
            quadIndex.insert(geometry);
        }
    }

    public MultiPolygon getOriginal() {
        Polygon[] polygons = new Polygon[this.geometries.length];
        for (int i = 0; i < this.geometries.length; i++) {
            polygons[i] = (Polygon) this.geometries[i];
        }
        return new MultiPolygon(polygons, factory);
    }

    public static QuadSplitedMultiPolygon fromGeom(MultiPolygon multiPolygon, List<Geometry> geometries) {
        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            polygons[i] = (Polygon) multiPolygon.getGeometryN(i);
        }
        return new QuadSplitedMultiPolygon(polygons, multiPolygon.getFactory(), geometries);
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
        if (other instanceof QuadSplitedMultiPolygon) {
            QuadSplitedMultiPolygon otherPolygon = (QuadSplitedMultiPolygon) other;
            return super.equals(other) && quadIndex.equals(otherPolygon.quadIndex);
        }
        return false;
    }

}
