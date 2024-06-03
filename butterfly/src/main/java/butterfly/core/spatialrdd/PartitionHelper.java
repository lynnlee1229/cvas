package butterfly.core.spatialrdd;


import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.GeometryHelper;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.indexing.IndexHelper;
import cn.edu.whu.lynn.synopses.Summary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Function1;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/11
 **/
public class PartitionHelper implements Serializable {
    /**
     * Compute number of partitions for a partitioner given the partitioning criterion and the summary of the dataset.
     *
     * @param pcriterion the criterion used to define the number of partitions
     * @param value      the value associated with the criterion
     * @param summary    the summary of the dataset
     * @return the preferred number of partitions
     */
    public static int computeNumberOfPartitions(String pcriterion, long value, Summary summary) {
        return IndexHelper.computeNumberOfPartitions(pcriterion, value, summary);
    }

    public static SpatialPartitioner createPartitioner(JavaRDD<IFeature> features,
                                                       Class<? extends SpatialPartitioner> partitionerClass,
                                                       String pcriterion,
                                                       long pvalue,
                                                       org.apache.spark.api.java.function.Function<IFeature, Object> sizeFunction,
                                                       ButterflyOptions opts) {
        return IndexHelper.createPartitioner(features, partitionerClass, pcriterion, pvalue, sizeFunction, opts);
    }

    public static JavaRDD<IFeature> partition(JavaRDD<IFeature> rdd, Class<? extends SpatialPartitioner> spatialPartitioner, String pcriterion, long pvalue, Function<IFeature, Object> sizeFunction, ButterflyOptions opts) {
        if (sizeFunction == null)
            sizeFunction = IFeature::getStorageSize;
        SpatialPartitioner partitioner = createPartitioner(rdd, spatialPartitioner, pcriterion, pvalue, sizeFunction, opts);
        return IndexHelper.partitionFeatures2(rdd, partitioner);
    }


    public static Summary computeSummary(JavaRDD<IFeature> featureRDD, Function1<IFeature, Object> sizeFunction) {
        return Summary.computeForFeaturesWithSize(featureRDD, sizeFunction);
    }

    public static JavaRDD<Tuple2<Integer, Summary>> computePartialSummaries(JavaRDD<IFeature> featureRDD, Function<IFeature, Object> sizeFunction) {
        JavaRDD<Tuple2<Integer, Summary>> tuple2JavaRDD = featureRDD.mapPartitionsWithIndex((idx, features) -> {
            Summary summary = new Summary();
            while (features.hasNext()) {
                IFeature feature = features.next();
                if (summary.getCoordinateDimension() == 0) {
                    summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(feature.getGeometry()));
                }
                summary.expandToGeometryWithSize(feature.getGeometry(), (Integer) sizeFunction.call(feature));
            }
            List<Tuple2<Integer, Summary>> summaries = new ArrayList<>();
            summaries.add(new Tuple2<>(idx, summary));
            return summaries.iterator();
        }, true);
        // If the number of partitions is very large, Spark would fail because it will try to collect all
        // the partial summaries locally and would run out of memory in such case
        if (tuple2JavaRDD.getNumPartitions() > 1000) {
            tuple2JavaRDD = tuple2JavaRDD.coalesce(1000);
        }
        return tuple2JavaRDD;
    }
}
