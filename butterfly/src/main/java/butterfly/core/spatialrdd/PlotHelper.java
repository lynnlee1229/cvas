package butterfly.core.spatialrdd;

import butterfly.core.common.ButterflyConstant;
import butterfly.core.utils.GeoUtils;
import cn.edu.whu.lynn.core.SpatialPartitioner;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.EnvelopeNDLite;
import cn.edu.whu.lynn.geolite.Feature;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.synopses.Summary;
import cn.edu.whu.lynn.davinci.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple3;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static butterfly.core.common.ButterflyConstant.worldMercatorMBR;

/**
 * @author Lynn Lee
 * @date 2024/1/11
 **/
public class PlotHelper {

    public static void plotImage(VectorRDD<Geometry> vectorRDD, int imageWidth, int imageHeight, String imagePath) {
        plotImage(vectorRDD, imageWidth, imageHeight, imagePath, GeometricPlotter.class, new ButterflyOptions());
    }

    public static void plotImage(VectorRDD<Geometry> vectorRDD, int imageWidth, int imageHeight, String imagePath,
                                 Class<? extends Plotter> plotterClass) {
        plotImage(vectorRDD, imageWidth, imageHeight, imagePath, plotterClass, new ButterflyOptions());
    }

    public static void plotImage(VectorRDD<Geometry> vectorRDD, int imageWidth, int imageHeight, String imagePath,
                                 Class<? extends Plotter> plotterClass, ButterflyOptions opts) {
        JavaRDD<IFeature> features = vectorRDD.getFeatureRDD();
//        PlotHelper.plotImage(vectorRDD, imageWidth, imageHeight, imagePath, plotterClass, opts);
        SingleLevelPlot.plotFeatures(features.rdd(), imageWidth, imageHeight, imagePath, plotterClass, vectorRDD.summary(), opts);
    }

    public static void plotFeatures(JavaRDD<IFeature> features, int imageWidth, int imageHeight,
                                    String imagePath, Class<? extends Plotter> plotterClass, EnvelopeNDLite canvasMBR,
                                    ButterflyOptions opts) {
        Tuple3<EnvelopeNDLite, Integer, Integer> tmp = calculateAdjustedSize(canvasMBR, imageWidth, imageHeight, features, opts);
        EnvelopeNDLite summary = tmp._1();
        int width = tmp._2();
        int height = tmp._3();
        Canvas finalCanvas = plotToCanvas(features, width, height, plotterClass, summary, opts);
        Plotter finalPlotter = createPlotterInstance(plotterClass, opts);
        plotCanvas(finalCanvas, imagePath, opts, finalPlotter, features.rdd().context().hadoopConfiguration());
    }

    public static void plotCanvas(List<Canvas> canvasList, String imagePath, ButterflyOptions opts, Plotter finalPlotter, Configuration hadoopConf) {
        Canvas canvas = mergeCanvas(canvasList, finalPlotter);
        Path outPath = new Path(imagePath);
        FileSystem outFileSystem = null;
        try {
            outFileSystem = outPath.getFileSystem(hadoopConf);
            FSDataOutputStream outStream = outFileSystem.create(outPath);
            finalPlotter.writeImage(canvas, outStream, opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true));
            outStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void plotCanvas(Canvas canvas, String imagePath, ButterflyOptions opts, Plotter finalPlotter, Configuration hadoopConf) {
        Path outPath = new Path(imagePath);
        FileSystem outFileSystem = null;
        try {
            outFileSystem = outPath.getFileSystem(hadoopConf);
            FSDataOutputStream outStream = outFileSystem.create(outPath);
            finalPlotter.writeImage(canvas, outStream, opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true));
            outStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Canvas mergeCanvas(List<Canvas> canvasList, Plotter plotter) {
        return canvasList.stream().reduce(plotter::merge).get();
    }

    public static Canvas plotToCanvas(JavaRDD<IFeature> features, int width, int height, Class<? extends Plotter> plotterClass, EnvelopeNDLite summary, ButterflyOptions opts) {
        JavaRDD<Canvas> partialCanvases = plotToCanvasByPartition(features, width, height, plotterClass, summary, opts);
        Plotter finalPlotter = createPlotterInstance(plotterClass, opts);
        Canvas finalCanvas = partialCanvases.reduce(finalPlotter::merge);
        boolean plotPartitionMBR = opts.getBoolean(ButterflyConstant.PLOT_PARTITION_MBR, false);
        if (plotPartitionMBR) {
            Partitioner partitioner = features.partitioner().get();
            if (partitioner instanceof SpatialPartitioner) {
                SpatialPartitioner spatialPartitioner = (SpatialPartitioner) partitioner;
                for (int i = 0; i < features.getNumPartitions(); i++) {
                    EnvelopeNDLite partitionMBR = spatialPartitioner.getPartitionMBR(i);
                    partitionMBR.shrink(worldMercatorMBR);
                    finalPlotter.plot(finalCanvas, toFeature(partitionMBR));
                }
                finalPlotter.plot(finalCanvas, toFeature(spatialPartitioner.getEnvelope()));
            }
        }
        return finalCanvas;
    }

    private static Tuple3<EnvelopeNDLite, Integer, Integer> calculateAdjustedSize(EnvelopeNDLite canvasMBR, int imageWidth, int imageHeight, JavaRDD<IFeature> features, ButterflyOptions opts) {
        EnvelopeNDLite summary = (canvasMBR != null) ? canvasMBR : Summary.computeForFeatures(features);

        int width;
        int height;

        if (opts.getBoolean(CommonVisualizationHelper.KeepRatio, true)) {
            int adjustedWidth = (int) (imageHeight * summary.getSideLength(0) / summary.getSideLength(1));
            int adjustedHeight = (int) (imageWidth * summary.getSideLength(1) / summary.getSideLength(0));
            width = Math.min(imageWidth, adjustedWidth);
            height = Math.min(imageHeight, adjustedHeight);
        } else {
            height = imageHeight;
            width = imageWidth;
        }
        return new Tuple3<>(summary, width, height);
    }

    public static JavaRDD<Canvas> plotToCanvasByPartition(JavaRDD<IFeature> features, int imageWidth, int imageHeight,
                                                          Class<? extends Plotter> plotterClass, EnvelopeNDLite canvasMBR,
                                                          ButterflyOptions opts) {
        EnvelopeNDLite summary;
        int width;
        int height;
        if (canvasMBR == null) {
            Tuple3<EnvelopeNDLite, Integer, Integer> tmp = calculateAdjustedSize(null, imageWidth, imageHeight, features, opts);
            summary = (EnvelopeNDLite) tmp._1();
            width = (int) tmp._2();
            height = (int) tmp._3();
        } else {
            height = imageHeight;
            width = imageWidth;
            summary = canvasMBR;
        }
        return features.mapPartitionsWithIndex((idx, iFeatureIterator) -> {
            boolean plotByPartition = opts.getBoolean(ButterflyConstant.PLOT_BY_PARTITION, false);
            if (plotByPartition) {
                String hexColor = generateHexColor(idx);
                opts.set("fill", hexColor);
            }
            Plotter plotter = createPlotterInstance(plotterClass, opts);
            Envelope canvasEnvelope = summary.toJTSEnvelope();
            Canvas canvas = plotter.createCanvas(width, height, canvasEnvelope, 0);
            while (iFeatureIterator.hasNext()) {
                IFeature feature = iFeatureIterator.next();
                plotter.plot(canvas, feature);
            }
            return Collections.singletonList(canvas).iterator();
        }, true);
    }

    private static IFeature toFeature(EnvelopeNDLite envelopeNDLite) {
        double minX = envelopeNDLite.getMinCoord(0);
        double minY = envelopeNDLite.getMinCoord(1);
        double maxX = envelopeNDLite.getMaxCoord(0);
        double maxY = envelopeNDLite.getMaxCoord(1);
        return Feature.create(null, GeoUtils.createRectangle(minX, minY, maxX, maxY));
    }


    public static String generateHexColor(int partitionId) {
        // 确保分区ID为正整数
        int positivePartitionId = Math.abs(partitionId);

        // 使用随机数生成RGB值
        Random random = new Random(positivePartitionId);
        int red = random.nextInt(256);
        int green = random.nextInt(256);
        int blue = random.nextInt(256);

        // 将RGB值转换为十六进制字符串
        String hexColor = String.format("#%02X%02X%02X", red, green, blue);

        return hexColor;
    }

//    private static JavaRDD<Canvas> createPartialCanvases(JavaRDD<IFeature> features, Class<? extends Plotter> plotterClass,
//                                                         EnvelopeNDLite summary, int width, int height, BeastOptions opts) {
//        return features.mapPartitionsWithIndex((idx, iFeatureIterator) -> {
//            boolean plotByPartition = opts.getBoolean(ButterflyConstant.PLOT_BY_PARTITION, false);
//            if (plotByPartition) {
//                String hexColor = generateHexColor(idx);
//                opts.set("fill", hexColor);
//            }
//            Plotter plotter = createPlotterInstance(plotterClass, opts);
//            Envelope canvasEnvelope = summary.toJTSEnvelope();
//            Canvas canvas = plotter.createCanvas(width, height, canvasEnvelope, 0);
//            while (iFeatureIterator.hasNext()) {
//                IFeature feature = iFeatureIterator.next();
//                plotter.plot(canvas, feature);
//            }
//            return Collections.singletonList(canvas).iterator();
//        }, true);
//    }

    public static Plotter createPlotterInstance(Class<? extends Plotter> plotterClass, ButterflyOptions opts) {
        try {
            Plotter plotter = plotterClass.getConstructor().newInstance();
            plotter.setup(opts);
            return plotter;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

//    public static void plotFeatures(JavaRDD<IFeature> features, int imageWidth, int imageHeight,
//                                    String imagePath, Class<? extends Plotter> plotterClass, EnvelopeNDLite canvasMBR,
//                                    BeastOptions opts) {
//        EnvelopeNDLite summary = (canvasMBR != null) ? canvasMBR : Summary.computeForFeatures(features);
//
//        int width;
//        int height;
//
//        if (opts.getBoolean(CommonVisualizationHelper.KeepRatio, true)) {
//            int adjustedWidth = (int) (imageHeight * summary.getSideLength(0) / summary.getSideLength(1));
//            int adjustedHeight = (int) (imageWidth * summary.getSideLength(1) / summary.getSideLength(0));
//            width = Math.min(imageWidth, adjustedWidth);
//            height = Math.min(imageHeight, adjustedHeight);
//        } else {
//            height = imageHeight;
//            width = imageWidth;
//        }
//
//        JavaRDD<Canvas> partialCanvases = features.mapPartitions(iFeatureIterator -> {
//            Plotter plotter = plotterClass.getConstructor().newInstance();
//            plotter.setup(opts);
//            Envelope canvasEnvelope = summary.toJTSEnvelope();
//            Canvas canvas = plotter.createCanvas(width, height, canvasEnvelope, 0);
//
//            while (iFeatureIterator.hasNext()) {
//                IFeature feature = iFeatureIterator.next();
//                plotter.plot(canvas, feature);
//            }
//
//            return Collections.singletonList(canvas).iterator();
//        });
//
//        Plotter plotter = null;
//        try {
//            plotter = plotterClass.getConstructor().newInstance();
//        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
//                 NoSuchMethodException e) {
//            throw new RuntimeException(e);
//        }
//        plotter.setup(opts);
//        Plotter finalPlotter = plotter;
//        Canvas finalCanvas = partialCanvases.reduce(finalPlotter::merge);
//
//        Path outPath = new Path(imagePath);
//        FileSystem outFileSystem = null;
//        try {
//            outFileSystem = outPath.getFileSystem(features.rdd().context().hadoopConfiguration());
//            FSDataOutputStream outStream = outFileSystem.create(outPath);
//            plotter.writeImage(finalCanvas, outStream, opts.getBoolean(CommonVisualizationHelper.VerticalFlip, true));
//            outStream.close();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
