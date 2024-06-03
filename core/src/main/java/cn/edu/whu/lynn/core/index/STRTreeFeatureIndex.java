package cn.edu.whu.lynn.core.index;

import cn.edu.whu.lynn.geolite.IFeature;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class STRTreeFeatureIndex implements FeatureIndex<IFeature>{
    private static final int DEFAULT_NODE_CAPACITY = 2;
    private final STRtree stRtree;

    public STRTreeFeatureIndex() {
        this.stRtree = new STRtree(DEFAULT_NODE_CAPACITY);
    }

    @Override
    public void insert(List geometries) {

    }

    @Override
    public void insert(IFeature geom) {
        stRtree.insert(geom.getGeometry().getEnvelopeInternal(), geom);
    }

    @Override
    public List<IFeature> query(Envelope envelope) {
        return stRtree.query(envelope);
    }

    @Override
    public List<IFeature> query(IFeature geometry) {
        return query(geometry.getGeometry().getEnvelopeInternal());
    }

    @Override
    public void remove(IFeature geom) {
        stRtree.remove(geom.getGeometry().getEnvelopeInternal(), geom);
    }

    @Override
    public int size() {
        return stRtree.size();
    }
}
