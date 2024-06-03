package butterfly.core.spatialPartitioner;

import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.geolite.EnvelopeNDLite;
import cn.edu.whu.lynn.synopses.AbstractHistogram;
import cn.edu.whu.lynn.synopses.Summary;
import cn.edu.whu.lynn.util.IntArray;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Lynn Lee
 * @date 2024/1/11
 **/
public class QudaTreeParttioner extends SpatialPartitioner {
    @Override
    public void construct(Summary summary, double[][] sample, AbstractHistogram histogram, int numPartitions) {

    }

    @Override
    public void overlapPartitions(EnvelopeNDLite mbr, IntArray matchedPartitions) {

    }

    @Override
    public int overlapPartition(EnvelopeNDLite mbr) {
        return 0;
    }

    @Override
    public void getPartitionMBR(int partitionID, EnvelopeNDLite mbr) {

    }

    @Override
    public int numPartitions() {
        return 0;
    }

    @Override
    public boolean isDisjoint() {
        return false;
    }

    @Override
    public int getCoordinateDimension() {
        return 0;
    }

    @Override
    public EnvelopeNDLite getEnvelope() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
