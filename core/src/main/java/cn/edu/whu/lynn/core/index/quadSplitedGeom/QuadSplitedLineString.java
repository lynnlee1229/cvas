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
public class QuadSplitedLineString extends LineString implements Serializable, QuadSplitedGeom {
    GeometryIndex<Geometry> quadIndex = new STRTreeGeometryIndex<>();

    public QuadSplitedLineString(CoordinateSequence points, GeometryFactory factory, List<Geometry> geometries) {
        super(points, factory);
        for (Geometry geometry : geometries) {
            quadIndex.insert(geometry);
        }
    }


    public LineString getOriginal() {
        return new LineString(points, factory);
    }

    public static QuadSplitedLineString fromGeom(LineString lineString, List<Geometry> geometries) {
        return new QuadSplitedLineString(lineString.getCoordinateSequence(), lineString.getFactory(), geometries);
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
        if (other instanceof QuadSplitedLineString) {
            QuadSplitedLineString otherLineString = (QuadSplitedLineString) other;
            return super.equals(other) && quadIndex.equals(otherLineString.quadIndex);
        }
        return false;
    }
}
