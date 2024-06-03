package cn.edu.whu.lynn.core.index.quadSplitedGeom;

import cn.edu.whu.lynn.core.index.GeometryIndex;
import cn.edu.whu.lynn.core.index.STRTreeGeometryIndex;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class QuadSplitedMultiLineString extends MultiLineString implements Serializable, QuadSplitedGeom{
    GeometryIndex<Geometry> quadIndex = new STRTreeGeometryIndex<>();

    public QuadSplitedMultiLineString(LineString[] lineStrings, GeometryFactory factory, List<Geometry> geometries) {
        super(lineStrings, factory);
        for (Geometry geometry : geometries) {
            quadIndex.insert(geometry);
        }
    }


    public MultiLineString getOriginal() {
        LineString[] lineStrings = new LineString[this.geometries.length];
        for (int i = 0; i < this.geometries.length; i++) {
            lineStrings[i] = (LineString) this.geometries[i];
        }
        return new MultiLineString(lineStrings, factory);
    }

    public static QuadSplitedMultiLineString fromGeom(MultiLineString multiLineString, List<Geometry> geometries) {
        LineString[] lineStrings = new LineString[multiLineString.getNumGeometries()];
        for (int i = 0; i < multiLineString.getNumGeometries(); i++) {
            lineStrings[i] = (LineString) multiLineString.getGeometryN(i);
        }
        return new QuadSplitedMultiLineString(lineStrings, multiLineString.getFactory(), geometries);
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
        if (other instanceof QuadSplitedMultiLineString) {
            QuadSplitedMultiLineString otherMultiLineString = (QuadSplitedMultiLineString) other;
            return super.equals(other) && quadIndex.equals(otherMultiLineString.quadIndex);
        }
        return false;
    }
}
