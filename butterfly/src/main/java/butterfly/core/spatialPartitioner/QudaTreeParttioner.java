package butterfly.core.spatialPartitioner;

import edu.ucr.cs.bdlab.beast.core.SpatialPartitioner;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeNDLite;
import edu.ucr.cs.bdlab.beast.synopses.AbstractHistogram;
import edu.ucr.cs.bdlab.beast.synopses.Summary;
import edu.ucr.cs.bdlab.beast.util.IntArray;

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
