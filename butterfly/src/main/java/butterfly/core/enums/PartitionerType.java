package butterfly.core.enums;

import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.indexing.*;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2024/1/8
 **/
public enum PartitionerType implements Serializable {
    /**
     * Grid based
     */
    GRID(GridPartitioner.class),
    CELL(CellPartitioner.class),
    ZCURVE(ZCurvePartitioner.class),
    HCURVE(HCurvePartitioner.class),
    STR(STRPartitioner.class),
    KDTree(KDTreePartitioner.class),
    RGROVE(RGrovePartitioner.class),
    RSGROVE(RSGrovePartitioner.class),
    RRSGROVE(RRSGrovePartitioner.class);
    private final Class<? extends SpatialPartitioner> partitionerClass;

    PartitionerType(Class<? extends SpatialPartitioner> partitionerClass) {
        this.partitionerClass = partitionerClass;
    }

    public Class<? extends SpatialPartitioner> getPartitionerClass() {
        return partitionerClass;
    }

    public static PartitionerType getPartitionerType(String str)
    {
        for (PartitionerType me : PartitionerType.values()) {
            if (me.name().equalsIgnoreCase(str)) { return me; }
        }
        return null;
    }
}
