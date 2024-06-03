package butterfly.core.spatialIndex.index;

import butterfly.core.utils.distance.DistanceCalculator;
import cn.edu.whu.lynn.geolite.EnvelopeNDLite;
import cn.edu.whu.lynn.geolite.IFeature;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/21
 **/
public abstract class SpatialIndexBase<T extends Geometry> implements IGeomSpatialIndex<T> {

    public void insertFeature(List geometries) {
        for (Object geom : geometries) {
            this.insertFeature((IFeature) geom);
        }
    }

    public void insertFeature(IFeature geom) {
        this.insert((T) geom.getGeometry());
    }

    public List query(EnvelopeNDLite envelope) {
        return this.query(envelope.toJTSEnvelope());
    }

    public List query(IFeature geometry) {
        return this.query(geometry.getGeometry());
    }

    public List query(IFeature geom, double distance, DistanceCalculator calculator) {
        return this.query(geom.getGeometry(), distance, calculator);
    }

    public void removeFeature(IFeature geom) {
        this.remove((T) geom.getGeometry());
    }

}
