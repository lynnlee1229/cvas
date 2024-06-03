package butterfly.core.spatialrdd;

import butterfly.core.common.ButterflyConstant;
import butterfly.core.enums.Distribution;
import butterfly.core.enums.FileType;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import butterfly.core.enums.PartitionerType;
import cn.edu.whu.lynn.JavaSpatialRDDHelper;
import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.*;
import cn.edu.whu.lynn.io.SpatialWriter;
import cn.edu.whu.lynn.synopses.Summary;
import cn.edu.whu.lynn.davinci.GeometricPlotter;
import cn.edu.whu.lynn.davinci.Plotter;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2023/12/8
 **/
public class VectorRDD<T extends Geometry> implements Serializable {
    // for adapt to beast
    private JavaRDD<IFeature> featureRDD;
    private JavaPairRDD<Integer, IFeature> partitionedFeatureRDD;

    private boolean partitioned = false;
    private boolean indexed = false;

    //    JavaSpatialSparkContext javaSpatialSparkContext;
    private ButterflySparkContext butterflySparkContext;
    // for adapt to sedona
    private SpatialRDD<T> spatialRDD;

    // vector RDD
    private JavaSparkContext javaSparkContext;
    private JavaRDD<T> vectorRDD;

    public VectorRDD(JavaRDD<IFeature> featureRDD) {
        this.featureRDD = featureRDD;
        this.vectorRDD = featureRDD.map(f -> (T) f.getGeometry());
    }

    public VectorRDD(SpatialRDD<T> spatialRDD) {
        this.spatialRDD = spatialRDD;
        this.vectorRDD = spatialRDD.getRawSpatialRDD();
    }

    public VectorRDD(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
        this.butterflySparkContext = new ButterflySparkContext(javaSparkContext);
    }

    public JavaRDD<IFeature> getFeatureRDD() {
        return featureRDD;
    }

    public JavaRDD<T> getVectorRDD() {
        if (vectorRDD != null) return vectorRDD;
        if (featureRDD != null) return featureRDD.map(f -> (T) f.getGeometry());
        if (spatialRDD != null) return spatialRDD.getRawSpatialRDD();
        return null;
    }

    public JavaPairRDD<Integer, IFeature> getPartitionedFeatureRDD() {
        return partitionedFeatureRDD;
    }

    public SpatialRDD<T> getSpatialRDD() {
        return spatialRDD;
    }

    public boolean isSpatiallyPartitioned() {
        boolean present = this.featureRDD.partitioner().isPresent();
        if (present) {
            return this.featureRDD.partitioner().get() instanceof SpatialPartitioner;
        }
        return false;
    }

    /**
     * io
     */


    public void fromWKTFile(String filePath, Object wktColumn, char delimiter, boolean header) {
        this.featureRDD = butterflySparkContext.readWKTFile(filePath, wktColumn, delimiter, header);
    }

    public void fromCSVPoint(String filePath, Object xColumn, Object yColumn, char delimiter, boolean header) {
        this.featureRDD = butterflySparkContext.readCSVPoint(filePath, xColumn, yColumn, delimiter, header);
    }

    public void fromShapefile(String filePath) {
        this.featureRDD = butterflySparkContext.shapefile(filePath);
    }

    public void fromPointGenerator(int cardinality, Distribution distributionType, Integer numPartitions, ButterflyOptions options) {
        if (numPartitions == null)
            this.featureRDD = butterflySparkContext.generatePointData(distributionType, cardinality, ButterflyConstant.DEFAULT_PARTITION_NUM, options);
        else
            this.featureRDD = butterflySparkContext.generatePointData(distributionType, cardinality, numPartitions, options);
    }

    public void fromBoxGenerator(int cardinality, Distribution distributionType, Integer numPartitions, ButterflyOptions options, List<Double> maxSizeArray) {
        if (numPartitions == null)
            this.featureRDD = butterflySparkContext.generateBoxData(distributionType, cardinality, ButterflyConstant.DEFAULT_PARTITION_NUM, options, maxSizeArray);
        else
            this.featureRDD = butterflySparkContext.generateBoxData(distributionType, cardinality, numPartitions, options, maxSizeArray);
    }

    public void fromPolygonGenerator(int cardinality, Distribution distributionType, Integer numPartitions, ButterflyOptions options, Double maxSize, Integer numSegments) {
        if (numPartitions == null)
            this.featureRDD = butterflySparkContext.generatePolygonData(distributionType, cardinality, ButterflyConstant.DEFAULT_PARTITION_NUM, options, maxSize, numSegments);
        else
            this.featureRDD = butterflySparkContext.generatePolygonData(distributionType, cardinality, numPartitions, options, maxSize, numSegments);
    }

    public void savePartition(String filename, ButterflyOptions opts) {
        assert featureRDD != null && partitioned;
        String oformat = opts.getString(SpatialWriter.OutputFormat(), null);
        if (oformat == null) {
            opts.set(SpatialWriter.OutputFormat(), "wkt(0)");
        }
        IndexHelper.saveIndex2J(this.featureRDD, filename, opts);
    }

    public VectorRDD(JavaSparkContext javaSparkContext, String filePath, FileType fileType, ButterflyConfiguration conf) {
        this.javaSparkContext = javaSparkContext;
        this.butterflySparkContext = new ButterflySparkContext(javaSparkContext);
        // TODO 更多的文件类型支持
        switch (fileType) {
            case WKT:
                this.featureRDD = butterflySparkContext.readWKTFile(filePath, "geometry", ',', true);
                break;
            default:
                throw new IllegalArgumentException("Unsupported file type:" + fileType);
        }
    }


    /**
     * op summary
     */

    public Summary summary() {
//        return summary(IFeature::getStorageSize);
        return JavaSpatialRDDHelper.summary(featureRDD);
    }

//    public Summary summary(Function1<IFeature, Object> sizeFunction) {
//        // TODO 检查这里为什么会报task无法序列化的错误
//        assert featureRDD != null;
//        return null;
////        return PartitionHelper.computeSummary(featureRDD, sizeFunction);
//    }

    public JavaRDD<Tuple2<Integer, Summary>> partitionSummary(Function<IFeature, Object> sizeFunction) {
        assert featureRDD != null && partitioned;
        return PartitionHelper.computePartialSummaries(featureRDD, sizeFunction);
    }

    public JavaRDD<Tuple2<Integer, Summary>> partitionSummary() {
        return partitionSummary(IFeature::getStorageSize);
    }

    /**
     * op partition
     */
    public void doPartition1(PartitionerType partitioner, int numPartitions) {
        doPartition1(partitioner, numPartitions, new ButterflyOptions());
    }

    public void doPartition1(PartitionerType partitioner, int numPartitions, ButterflyOptions options) {
        if (partitioned) return;
        assert featureRDD != null;
        switch (partitioner) {
            case GRID:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, GridPartitioner.class, numPartitions, options);
                break;
            case CELL:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, CellPartitioner.class, numPartitions, options);
                break;
            case ZCURVE:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, ZCurvePartitioner.class, numPartitions, options);
                break;
            case HCURVE:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, HCurvePartitioner.class, numPartitions, options);
                break;
            case STR:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, STRPartitioner.class, numPartitions, options);
                break;
            case KDTree:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, KDTreePartitioner.class, numPartitions, options);
                break;
            case RGROVE:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, RGrovePartitioner.class, numPartitions, options);
                break;
            case RSGROVE:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, RSGrovePartitioner.class, numPartitions, options);
                break;
            case RRSGROVE:
                partitionedFeatureRDD = JavaSpatialRDDHelper.spatialPartition(featureRDD, RRSGrovePartitioner.class, numPartitions, options);
                break;
            default:
                throw new IllegalArgumentException("Unsupported partitioner type:" + partitioner);
        }
        partitioned = true;
    }

    public void doPartition2(Class<? extends SpatialPartitioner> partitioner, String type, int value) {
        doPartition2(partitioner, type, value, null, new ButterflyOptions());
    }

    public void doPartition2(Class<? extends SpatialPartitioner> partitioner, String type, int value, org.apache.spark.api.java.function.Function<IFeature, Object> sizeFunction, ButterflyOptions options) {
        if (partitioned) return;
        assert featureRDD != null;
        featureRDD = PartitionHelper.partition(featureRDD, partitioner, type, value, sizeFunction, options);
        partitioned = true;
    }

    /**
     * op plot
     */
    public void plotImage(int imageWidth, int imageHeight, String imagePath, Class<? extends Plotter> plotterClass, ButterflyOptions opts) {
//        SingleLevelPlot.plotFeatures(this.featureRDD.rdd(), imageWidth, imageHeight, imagePath, plotterClass, this.summary(), opts);
        PlotHelper.plotFeatures(this.featureRDD, imageWidth, imageHeight, imagePath, plotterClass, null, opts);
    }

    public void plotImage(int imageWidth, int imageHeight, String imagePath, ButterflyOptions opts) {
        plotImage(imageWidth, imageHeight, imagePath, GeometricPlotter.class, opts);
        //        SingleLevelPlot.plotFeatures(this.featureRDD.rdd(), imageWidth, imageHeight, imagePath, GeometricPlotter.class, this.summary(), opts);

    }

    /**
     * op sample
     */
    public JavaRDD<IFeature> sampleFeatures(boolean withReplacement, double fraction, long seed) {
        return featureRDD.sample(withReplacement, fraction, seed);
    }

    public JavaRDD<IFeature> sampleFeatures(boolean withReplacement, double fraction) {
        return featureRDD.sample(withReplacement, fraction, 1229);
    }
}

