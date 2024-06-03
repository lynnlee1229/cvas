package butterfly.core.utils;


import butterfly.core.base.mbr.MinimumBoundingBox;
import butterfly.core.base.point.BasePoint;
import butterfly.core.base.point.TrajPoint;
import cn.edu.whu.lynn.geolite.GeometryHelper;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.ShapeFactory;

import java.io.Serializable;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2022/9/8
 **/
public class GeoUtils implements Serializable {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    public static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;
    public static final DistanceCalculator DISTANCE_CALCULATOR = SPATIAL_CONTEXT.getDistCalc();
    public static final ShapeFactory SHAPE_FACTORY = SPATIAL_CONTEXT.getShapeFactory();

    private static final double MIN_LNG = -180.;
    private static final double MAX_LNG = 180.;
    private static final double MIN_LAT = -90.;
    private static final double MAX_LAT = 90.;

    /**
     * Creates a point from the given coordinate.
     * ST_Point in Sedona Spark API took an optional z value before v1.4.0.
     * This was removed to avoid confusion with other GIS implementations where the optional third argument is srid.
     * <p>
     * A future version of Sedona will add a srid parameter once enough users have upgraded and hence are forced
     * to use ST_PointZ for 3D points.
     *
     * @param x the x value
     * @param y the y value
     * @return The point geometry
     */
    public static Geometry point(double x, double y) {
        // See srid parameter discussion in https://issues.apache.org/jira/browse/SEDONA-234
        GeometryFactory geometryFactory = new GeometryFactory();
        return geometryFactory.createPoint(new Coordinate(x, y));
    }

    public static Geometry makePoint(Double x, Double y, Double z, Double m) {
        GeometryFactory geometryFactory = new GeometryFactory();
        if (x == null || y == null) {
            return null;
        }
        if (z == null && m == null) {
            return geometryFactory.createPoint(new Coordinate(x, y));
        }
        if (z != null && m == null) {
            return geometryFactory.createPoint(new Coordinate(x, y, z));
        }
        if (z == null) {
            return geometryFactory.createPoint(new CoordinateXYZM(x, y, 0, m));
        }
        return geometryFactory.createPoint(new CoordinateXYZM(x, y, z, m));
    }

    public static Polygon createRectangle(double minx, double miny, double maxx, double maxy) {
        return createRectangle(minx, miny, maxx, maxy, GEOMETRY_FACTORY);
    }


    public static Polygon createRectangle(double minx, double miny, double maxx, double maxy, GeometryFactory factory) {
        CoordinateSequence cs = factory.getCoordinateSequenceFactory().create(5, 2);
        cs.setOrdinate(0, 0, minx);
        cs.setOrdinate(0, 1, miny);

        cs.setOrdinate(1, 0, maxx);
        cs.setOrdinate(1, 1, miny);

        cs.setOrdinate(2, 0, maxx);
        cs.setOrdinate(2, 1, maxy);

        cs.setOrdinate(3, 0, minx);
        cs.setOrdinate(3, 1, maxy);

        cs.setOrdinate(4, 0, minx);
        cs.setOrdinate(4, 1, miny);
        return factory.createPolygon(cs);
    }

    public static List<Geometry> quadSplit(Geometry geometry, int threshold) {
        assert GeometryHelper.getCoordinateDimension(geometry) == 2 : "This function only works with 2D geometries";
        Geometry esrGeometry = (geometry);
        List<Geometry> parts = new ArrayList<>();
        int numGeomsToCheck = 1;
        parts.add(esrGeometry);
        // Convert the threshold to an estimated memory size for simplicity to use with Esri API
        while (numGeomsToCheck > 0) {
            Geometry geomToCheck = parts.remove(0);
            numGeomsToCheck--;
            if (geomToCheck.getNumPoints() <= threshold) {
                // Already simple. Add to results
                parts.add(geomToCheck);
            } else {
                // A complex geometry, split into four
                Envelope mbr = geomToCheck.getEnvelopeInternal();
                double centerx = (mbr.getMinX() + mbr.getMaxX()) / 2.0;
                double centery = (mbr.getMinY() + mbr.getMaxY()) / 2.0;
                // First quadrant
                Polygon quadrant = createRectangle(mbr.getMinX(), mbr.getMinY(), centerx, centery, geomToCheck.getFactory());
                parts.add(geomToCheck.intersection(quadrant));
                // Second quadrant
                quadrant = createRectangle(centerx, mbr.getMinY(), mbr.getMaxX(), centery, geomToCheck.getFactory());
                parts.add(geomToCheck.intersection(quadrant));
                // Third quadrant
                quadrant = createRectangle(centerx, centery, mbr.getMaxX(), mbr.getMaxY(), geomToCheck.getFactory());
                parts.add(geomToCheck.intersection(quadrant));
                // Fourth quadrant
                quadrant = createRectangle(mbr.getMinX(), centery, centerx, mbr.getMaxY(), geomToCheck.getFactory());
                parts.add(geomToCheck.intersection(quadrant));
                numGeomsToCheck += 4;
            }
        }
        // Convert all parts back to lite geometry
        List<Geometry> results = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            results.add((parts.get(i)));
        }
        return results;
    }

    public static double calCurRate(Geometry geometry) {
        Geometry convexHull = geometry.convexHull();
        return (convexHull.getArea() - geometry.getArea()) / geometry.getArea();
    }

    public static Polygon generatePolygon(int sides) {
        CoordinateSequence cs = CoordinateArraySequenceFactory.instance().create(sides + 1, 2);
        for (int i = 0; i < sides; i++) {
            double angle = i * Math.PI * 2.0 / sides;
            cs.setOrdinate(i, 0, Math.cos(angle));
            cs.setOrdinate(i, 1, Math.sin(angle));
        }
        cs.setOrdinate(sides, 0, Math.cos(0));
        cs.setOrdinate(sides, 1, Math.sin(0));
        return (Polygon) (new GeometryFactory().createPolygon(cs));
    }

    public static double distanceToDEG(double distance) {
        return distance * DistanceUtils.KM_TO_DEG;
    }

    /**
     * Calculate distance of two geometries. If the geometry is not point, use the centroid
     * of the geometry to calculate.
     */
    public static double getEuclideanDistanceKM(Geometry geom1, Geometry geom2) {
        org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
        org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
        return getEuclideanDistanceKM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    public static double getEuclideanDistance(Geometry geom1, Geometry geom2, String unit) {
        if (unit == "m") {
            return getEuclideanDistanceM(geom1, geom2);
        }
        return getEuclideanDistanceKM(geom1, geom2);
    }

    public static double getEuclideanDistanceM(Geometry geom1, Geometry geom2) {
        org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
        org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
        return getEuclideanDistanceM(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    public static double getEuclideanDistanceKM(double lng1, double lat1, double lng2, double lat2) {
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLat = lat1Rad - lat2Rad;
        double deltaLng = Math.toRadians(lng1) - Math.toRadians(lng2);
        return 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(deltaLat / 2.0), 2.0)
                + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.pow(Math.sin(deltaLng / 2.0), 2.0)))
                * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM;
    }

    public static double getEuclideanDistanceM(double lng1, double lat1, double lng2, double lat2) {
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLat = lat1Rad - lat2Rad;
        double deltaLng = Math.toRadians(lng1) - Math.toRadians(lng2);
        return 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(deltaLat / 2.0), 2.0)
                + Math.cos(lat1Rad) * Math.cos(lat2Rad) * Math.pow(Math.sin(deltaLng / 2.0), 2.0)))
                * DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM * 1000;
    }

    public static double getEuclideanDistanceM(BasePoint p1, BasePoint p2) {
        return getEuclideanDistanceM(p1.getLng(), p1.getLat(), p2.getLng(), p2.getLat());
    }

    /**
     * Calculate the envelop.
     *
     * @param p   the center point
     * @param dis distance km
     */
    public static Envelope calcEnvelopeByDis(org.locationtech.jts.geom.Point p, double dis) {
        return calcEnvelopeByDis(p.getX(), p.getY(), dis);
    }

    public static Envelope calcEnvelopeByDis(double lng, double lat, double dis) {
        Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
        Rectangle rect = DISTANCE_CALCULATOR.calcBoxByDistFromPt(
                point, dis * DistanceUtils.KM_TO_DEG, SPATIAL_CONTEXT, null);
        return new Envelope(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
    }

    public static double getGeoListLen(List<Geometry> geoList) {
        double len = 0.0;
        for (int i = 1; i < geoList.size(); i++) {
            len += getEuclideanDistanceKM(geoList.get(i - 1).getCentroid(), geoList.get(i).getCentroid());
        }
        return len;
    }

    public static double getTrajListLen(List<TrajPoint> trajList) {
        double len = 0.0;
        for (int i = 1; i < trajList.size(); i++) {
            len +=
                    getEuclideanDistanceKM(trajList.get(i - 1).getCentroid(), trajList.get(i).getCentroid());
        }
        return len;
    }

    /**
     * Calculate the envelop.
     *
     * @param p   the center point
     * @param dis distance km
     */
    public static Envelope getEnvelopeByDis(org.locationtech.jts.geom.Point p, double dis) {
        return getEnvelopeByDis(p.getX(), p.getY(), dis);
    }

    public static Envelope getEnvelopeByDis(double lng, double lat, double dis) {
        Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
        Rectangle rect = DISTANCE_CALCULATOR.calcBoxByDistFromPt(point, dis * DistanceUtils.KM_TO_DEG,
                SPATIAL_CONTEXT, null);
        return new Envelope(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
    }

    public static Coordinate getPointOnBearing(double lng, double lat, double angle, double dis) {
        Point point = SHAPE_FACTORY.pointXY(checkLng(lng), checkLat(lat));
        Point result = DISTANCE_CALCULATOR.pointOnBearing(point, dis * DistanceUtils.KM_TO_DEG, angle,
                SPATIAL_CONTEXT, null);
        return new Coordinate(result.getX(), result.getY());
    }

    private static double checkLng(double lng) {
        if (lng < MIN_LNG) {
            return MIN_LNG;
        }
        return Math.min(lng, MAX_LNG);
    }

    private static double checkLat(double lat) {
        if (lat < MIN_LAT) {
            return MIN_LAT;
        }
        return Math.min(lat, MAX_LAT);
    }

    public static double getKmFromDegree(double degree) {
        return degree * DistanceUtils.DEG_TO_KM;
    }

    public static double getDegreeFromKm(double km) {
        return km * DistanceUtils.KM_TO_DEG;
    }

    public static double getSpeed(TrajPoint p1, TrajPoint p2) {
        long timeSpanInSec = Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p2.getTimestamp()));
        if (timeSpanInSec == 0L) {
            return 0.0;
        } else {
            double distanceInM = getEuclideanDistanceM(p1, p2);
            return distanceInM / (double) timeSpanInSec * 3.6;
        }
    }

    public static double getSpeed(TrajPoint p1, TrajPoint p2, TrajPoint p3) {
        long timeSpanInSec = Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p3.getTimestamp()));
        if (timeSpanInSec == 0L) {
            return 0.0;
        } else {
            double distanceInM = getEuclideanDistanceM(p1, p2) + getEuclideanDistanceM(p2, p3);
            return distanceInM / (double) timeSpanInSec * 3.6;
        }
    }

    public static double getDeltaV(TrajPoint p1, TrajPoint p2, TrajPoint p3) {
        long timeSpanInSec2 =
                Math.abs(ChronoUnit.SECONDS.between(p2.getTimestamp(), p3.getTimestamp()));
        if (timeSpanInSec2 == 0) {
            return 0.0;
        }
        long timeSpanInSec1 =
                Math.abs(ChronoUnit.SECONDS.between(p1.getTimestamp(), p3.getTimestamp()));
        double v1 = timeSpanInSec1 == 0 ? 0.0 : getEuclideanDistanceM(p1, p2) / (double) timeSpanInSec1;
        double v2 = getEuclideanDistanceM(p2, p3) / (double) timeSpanInSec2;
        double deltaV = (v2 - v1) / timeSpanInSec2;
        return (v2 - v1) / timeSpanInSec2;
    }

    public static MinimumBoundingBox calMinimumBoundingBox(List geoList) {
        if (geoList != null && !geoList.isEmpty()) {
            double latMin = Double.MAX_VALUE;
            double lngMin = Double.MAX_VALUE;
            double latMax = Double.MIN_VALUE;
            double lngMax = Double.MIN_VALUE;
            double tmpLng, tmpLat;
            for (Iterator iter = geoList.iterator(); iter.hasNext();
                 lngMax = Double.max(tmpLng, lngMax)) {
                BasePoint tmpP = (BasePoint) iter.next();
                tmpLat = tmpP.getLat();
                tmpLng = tmpP.getLng();
                latMin = Double.min(tmpLat, latMin);
                lngMin = Double.min(tmpLng, lngMin);
                latMax = Double.max(tmpLat, latMax);
            }

            return new MinimumBoundingBox(new BasePoint(lngMin, latMin), new BasePoint(lngMax, latMax));
        } else {
            return null;
        }
    }

    public static double getEuclideanDistance(BasePoint p0, BasePoint p1) {
        double dx = p1.getX() - p0.getX();
        double dy = p1.getY() - p0.getY();
        return Math.sqrt((dx * dx + dy * dy));
    }

    public static double getEuclideanDistance(double x0, double y0, double x1, double y1) {
        double dx = x1 - x0;
        double dy = y1 - y0;
        return Math.sqrt((dx * dx + dy * dy));
    }

    public static double getAngle(TrajPoint p0, TrajPoint p1, TrajPoint p2) {
        double d1 = getEuclideanDistance(p0, p1);
        double d2 = getEuclideanDistance(p1, p2);
        // 三点中至少有1静止点，返回180度
        if (d1 * d2 == 0) {
            return 180.0;
        }
        double x1 = p1.getX() - p0.getX();
        double y1 = p1.getY() - p0.getY();
        double x2 = p2.getX() - p1.getX();
        double y2 = p2.getY() - p1.getY();
        double delta = (x1 * x2 + y1 * y2) / (d1 * d2);
        // 边界值问题处理
        if (Math.abs(delta - 1.0) < 1e-10) {
            delta = 1.0;
        }
        if (Math.abs(delta + 1.0) < 1e-10) {
            delta = -1.0;
        }
        return Math.toDegrees(Math.acos(delta));
    }

    public static double getRatio(TrajPoint p0, TrajPoint p1, TrajPoint p2) {
        double d1 = getEuclideanDistanceM(p0, p1);
        double d2 = getEuclideanDistanceM(p1, p2);
        double d3 = getEuclideanDistanceM(p0, p2);
        return (d1 + d2) / d3;
    }

    public static TrajPoint fixPos(TrajPoint preP, TrajPoint curP, TrajPoint nextP) {
        double tSum = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), nextP.getTimestamp()));
        double t = Math.abs(ChronoUnit.SECONDS.between(preP.getTimestamp(), curP.getTimestamp()));
        if (t != 0) {
            curP.setLng(preP.getLng() + (nextP.getLng() - preP.getLng()) * t / tSum);
            curP.setLat(preP.getLat() + (nextP.getLat() - preP.getLat()) * t / tSum);

        } else {
            curP.setLng(preP.getLng());
            curP.setLat(preP.getLat());
        }
        return curP;
    }

    public static double getEuclideanDistanceM(MinimumBoundingBox rMbr, MinimumBoundingBox mbr) {
        if (rMbr.isIntersects(mbr)) {
            return 0.0;
        } else {
            BasePoint var2 = rMbr.getLowerLeft();
            BasePoint var14 = rMbr.getUpperRight();
            BasePoint var3 = mbr.getLowerLeft();
            BasePoint var15 = mbr.getUpperRight();
            double var6 = var2.getLat();
            double var8 = var2.getLng();
            double var10 = var3.getLat();
            double var12 = var3.getLng();
            if (var14.getLng() < var3.getLng()) {
                var8 = var14.getLng();
            } else if (var15.getLng() < var2.getLng()) {
                var12 = var15.getLng();
            } else {
                var12 = var2.getLng();
            }

            if (var14.getLat() < var3.getLat()) {
                var6 = var14.getLat();
            } else if (var2.getLat() < var15.getLat()) {
                var10 = var2.getLat();
            } else {
                var10 = var15.getLat();
            }

            return getEuclideanDistanceM(
                    new BasePoint(var8, var6), new BasePoint(var12, var10));
        }
    }

    public static double calcDistance(Geometry geom1, Geometry geom2) {
        org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
        org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
        return calcDistance(p1.getX(), p1.getY(), p2.getX(), p2.getY());
    }

    public static double calcDistance(double lng1, double lat1, double lng2, double lat2) {
        Point point1 = SHAPE_FACTORY.pointXY(checkLng(lng1), checkLat(lat1));
        Point point2 = SHAPE_FACTORY.pointXY(checkLng(lng2), checkLat(lat2));
        return SPATIAL_CONTEXT.calcDistance(point1, point2) * DistanceUtils.DEG_TO_KM;
    }
}
