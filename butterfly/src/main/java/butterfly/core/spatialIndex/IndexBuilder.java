package butterfly.core.spatialIndex;

import butterfly.core.enums.IndexType;
import butterfly.core.spatialIndex.index.IGeomSpatialIndex;
import butterfly.core.spatialIndex.index.STRTreeIndex;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Lynn Lee
 * @date 2023/11/15
 **/
public final class IndexBuilder<T extends Geometry>
        implements FlatMapFunction<Iterator<T>, IGeomSpatialIndex> {
    IndexType indexType;

    public IndexBuilder(IndexType indexType) {
        this.indexType = indexType;
    }

    // TODO 研究点：更多索引支持和实验
    @Override
    public Iterator<IGeomSpatialIndex> call(Iterator<T> objectIterator)
            throws Exception {
        IGeomSpatialIndex spatialIndex = null;
        switch (indexType) {
            case HPRTREE:
//                spatialIndex = new HPRtree();
                break;
            case STRTREE:
                spatialIndex = new STRTreeIndex();
                break;
            case QUADTREE:
//                spatialIndex = new Quadtree();
                break;
            default:
                throw new Exception("Unsupported index type: " + indexType);
        }

        while (objectIterator.hasNext()) {
            T spatialObject = objectIterator.next();
            spatialIndex.insert(spatialObject);
        }
        Set<IGeomSpatialIndex> result = new HashSet();
        spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0));
        result.add(spatialIndex);
        return result.iterator();
    }
}
