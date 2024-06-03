package butterfly.core.spatialrdd;

import butterfly.core.common.Key;
import butterfly.core.enums.Distribution;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.generator.*;
import cn.edu.whu.lynn.geolite.EnvelopeNDLite;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.io.CSVFeatureReader;
import cn.edu.whu.lynn.io.FeatureReader;
import cn.edu.whu.lynn.io.SpatialFileRDD;
import cn.edu.whu.lynn.io.SpatialReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2024/1/9
 **/
public class ButterflySparkContext extends JavaSparkContext {
    public ButterflySparkContext(SparkContext sc) {
        super(sc);
    }

    public ButterflySparkContext(String master, String appName) {
        super(master, appName);
    }

    public ButterflySparkContext(JavaSparkContext jsc) {
        super(jsc.sc());
    }

    public ButterflySparkContext(SparkConf conf) {
        super(conf);
    }

    public ButterflySparkContext(String master, String appName, SparkConf conf) {
        super(master, appName, conf);
    }

    public ButterflySparkContext(String master, String appName, String sparkHome, String[] jars) {
        super(master, appName, sparkHome, jars);
    }

    public ButterflySparkContext(String master, String appName, String sparkHome, String[] jars, Map<String, String> environment) {
        super(master, appName, sparkHome, jars, environment);
    }

    public JavaRDD<IFeature> spatialFile(String filename, String format, ButterflyOptions opts) {
        ButterflyOptions readOpts = new ButterflyOptions(opts);

        if (format == null) {
            // Try to auto-detect it
            Tuple2<Class<? extends FeatureReader>, ButterflyOptions> detectedOptions = SpatialFileRDD.autodetectInputFormat(
                    filename, readOpts.mergeWith(new ButterflyOptions(this.hadoopConfiguration())));

            if (detectedOptions == null)
                throw new RuntimeException("Cannot autodetect the format of the file '" + filename + "'");

            readOpts.mergeWith(detectedOptions._2());
        } else {
            readOpts.set(SpatialFileRDD.InputFormat(), format);
        }
        return SpatialReader.readInput(this, readOpts, filename, readOpts.getString(SpatialFileRDD.InputFormat()));
    }

    public JavaRDD<IFeature> spatialFile(String filename, ButterflyOptions opts) {
        return spatialFile(filename, opts.getString(SpatialFileRDD.InputFormat()), opts);
    }

    public JavaRDD<IFeature> shapefile(String filename) {
        return SpatialReader.readInput(this, new ButterflyOptions(), filename, "shapefile");
    }

    public JavaRDD<IFeature> geojson(String filename, ButterflyOptions opts) {
        return SpatialReader.readInput(this, new ButterflyOptions(), filename, "geojson");
    }

    public JavaRDD<IFeature> readCSVPoint(String filename, Object xColumn, Object yColumn, char delimiter,
                                          boolean skipHeader) {
        ButterflyOptions opts = new ButterflyOptions();
        opts.set(CSVFeatureReader.FieldSeparator, String.valueOf(delimiter));

        if (xColumn instanceof String || yColumn instanceof String) {
            opts.set(CSVFeatureReader.SkipHeader, true);
        } else {
            opts.set(CSVFeatureReader.SkipHeader, skipHeader);
        }

        String spatialFormat = String.format("point(%s,%s)", xColumn, yColumn);

        return SpatialReader.readInput(this, opts, filename, spatialFormat);
    }

    public JavaRDD<IFeature> readCSVPoint(String filename, Object xColumn, Object yColumn, String delimiter,
                                          boolean skipHeader) {
        ButterflyOptions opts = new ButterflyOptions();
        opts.set(CSVFeatureReader.FieldSeparator, delimiter);

        if (xColumn instanceof String || yColumn instanceof String) {
            opts.set(CSVFeatureReader.SkipHeader, true);
        } else {
            opts.set(CSVFeatureReader.SkipHeader, skipHeader);
        }
        String spatialFormat = String.format("point(%s,%s)", xColumn, yColumn);

        return SpatialReader.readInput(this, opts, filename, spatialFormat);
    }


    public JavaRDD<IFeature> readWKTFile(String filename, Object wktColumn, char delimiter,
                                         boolean skipHeader) {
        ButterflyOptions opts = new ButterflyOptions();
        opts.set(CSVFeatureReader.FieldSeparator, String.valueOf(delimiter));

        if (wktColumn instanceof String) {
            opts.set(CSVFeatureReader.SkipHeader, true);
        } else {
            opts.set(CSVFeatureReader.SkipHeader, skipHeader);
        }

        String spatialFormat = String.format("wkt(%s)", wktColumn);

        return SpatialReader.readInput(this, opts, filename, spatialFormat);
    }

    public JavaRDD<IFeature> readWKTFile(String filename, Object wktColumn, String delimiter,
                                         boolean skipHeader) {
        ButterflyOptions opts = new ButterflyOptions();
        opts.set(CSVFeatureReader.FieldSeparator, delimiter);

        if (wktColumn instanceof String) {
            opts.set(CSVFeatureReader.SkipHeader, true);
        } else {
            opts.set(CSVFeatureReader.SkipHeader, skipHeader);
        }

        String spatialFormat = String.format("wkt(%s)", wktColumn);

        return SpatialReader.readInput(this, opts, filename, spatialFormat);
    }

    public JavaRDD<IFeature> generatePointData(Distribution distribution, long cardinality,
                                               Integer numPartitions, ButterflyOptions opts) {
        return getSpatialGeneratorBuilder(distribution, numPartitions, opts)
                .generate(cardinality).toJavaRDD();
    }

    public JavaRDD<IFeature> generatePolygonData(Distribution distribution, long cardinality,
                                                 Integer numPartitions, ButterflyOptions opts, Double maxSize, Integer numSegments) {
        // 将maxSize限制在[0,1]
        maxSize = Math.min(Math.max(maxSize, 0), 1);
        return getSpatialGeneratorBuilder(distribution, numPartitions, opts).makePolygons(maxSize, numSegments)
                .generate(cardinality).toJavaRDD();
    }

    public JavaRDD<IFeature> generateBoxData(Distribution distribution, long cardinality,
                                             int numPartitions, ButterflyOptions opts, List<Double> maxSizeArray) {
        // 将maxSize限制在[0,1]
        for (int i = 0; i < maxSizeArray.size(); i++) {
            maxSizeArray.set(i, Math.min(Math.max(maxSizeArray.get(i), 0), 1));
        }
        return getSpatialGeneratorBuilder(distribution, numPartitions, opts)
                .makeBoxes((Seq<Object>) maxSizeArray).generate(cardinality).toJavaRDD();
    }

    public SpatialGeneratorBuilder getSpatialGeneratorBuilder(Distribution distribution
            , int numPartitions, ButterflyOptions opts) {
        // fixed seed for reproducibility
        if (!opts.contains(Key.SEED)) {
            opts.set(Key.SEED, 1229);
        }
        SpatialGeneratorBuilder spatialGeneratorBuilder = generateSpatialData(opts)
                .config(opts)
                .numPartitions(numPartitions);
        // mbr: minX,minY,maxX,maxY
        if (opts.contains(Key.MBR)) {
            String mbrStr = opts.getString(Key.MBR);
            String[] mbrArray = mbrStr.split(",");
            double[] mbr = new double[mbrArray.length];
            for (int i = 0; i < mbrArray.length; i++) {
                mbr[i] = Double.parseDouble(mbrArray[i]);
            }
            spatialGeneratorBuilder.mbr(new EnvelopeNDLite(2, mbr[0], mbr[1], mbr[2], mbr[3]));
        }
        switch (distribution) {
            case UNIFORM:
                spatialGeneratorBuilder.distribution(UniformDistribution$.MODULE$);
                break;
            case GAUSSIAN:
                spatialGeneratorBuilder.distribution(GaussianDistribution$.MODULE$);
                break;
            case DIAGONAL:
                spatialGeneratorBuilder.distribution(DiagonalDistribution$.MODULE$);
                break;
            case BIT:
                spatialGeneratorBuilder.distribution(BitDistribution$.MODULE$);
                break;
            case PARCELL:
                spatialGeneratorBuilder.distribution(ParcelDistribution$.MODULE$);
                break;
            case SIERPINSKI:
                spatialGeneratorBuilder.distribution(SierpinskiDistribution$.MODULE$);
                break;
            default:
                throw new RuntimeException("Unsupported distribution type");
        }
        return spatialGeneratorBuilder;
    }

    /**
     * Returns a random spatial generator factory
     *
     * @return a factory that can be used to generate spatial data
     */
    public SpatialGeneratorBuilder generateSpatialData(ButterflyOptions opts) {
        return new SpatialGeneratorBuilder(this.sc(), opts);
    }

}
