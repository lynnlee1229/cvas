package spatialrdd.ch2;

import butterfly.core.algrithm.join.JoinHelper;
import butterfly.core.common.Key;
import butterfly.core.common.PredefinedVectorRDD;
import butterfly.core.enums.Distribution;
import butterfly.core.spatialrdd.VectorRDD;
import cn.edu.whu.lynn.JavaSpatialSparkContext;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.core.SJAlgorithm;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.geolite.IFeature;
import cn.edu.whu.lynn.operations.SpatialJoinWithSimplification;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/1/8
 * 第二章Test：主要测试空间连接的内容
 **/
public class Chapter2Test implements Serializable {
    public final String OUT_PATH = "outs/ch2/";
    public JavaSpatialSparkContext jsc = new JavaSpatialSparkContext("local[*]", "chapter2Test");


    @Test
    public void complexSpatialJoinTest1() {
        // 通过连接测试复杂矢量计算效果
        int pointNum = 100000;
        int polygonNum = 10;
        double complexRate = 0; // 复杂矢量占比
        Distribution pointDistribution = Distribution.UNIFORM;
        Distribution polygonDistribution = Distribution.UNIFORM;
        int partitionNum = 8;
        int seed1 = 1229;
        int seed2 = 1230;
        double polygonMaxSize = 0.1;
        int normalPolygonMaxSegment = 10;
        int complexPolygonMaxSegment = 1000;
        PredefinedVectorRDD predefinedVectorRDD1 = new PredefinedVectorRDD(jsc);
        VectorRDD<Geometry> pointDataGen = predefinedVectorRDD1.getPointFromGenerator(pointNum, pointDistribution,
                partitionNum, new ButterflyOptions().set(Key.SEED, seed1));
        JavaRDD<IFeature> pointRDD = pointDataGen.getFeatureRDD();
        JavaRDD<IFeature> polygonRDD;
        if (complexRate != 0 || complexRate != 1) {
            PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
            PredefinedVectorRDD predefinedVectorRDD3 = new PredefinedVectorRDD(jsc);
            VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator((int) (polygonNum * (1 - complexRate)), polygonDistribution,
                    partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, normalPolygonMaxSegment);
            VectorRDD<Geometry> complexPolygonDataGen = predefinedVectorRDD3.getPolygonFromGenerator((int) (polygonNum * complexRate), polygonDistribution,
                    partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, complexPolygonMaxSegment);
            JavaRDD<IFeature> simplePolygonRDD = polygonDataGen.getFeatureRDD();
            JavaRDD<IFeature> complexPolygonRDD = complexPolygonDataGen.getFeatureRDD();
            // merge simple and complex polygon
            polygonRDD = simplePolygonRDD.union(complexPolygonRDD);
        } else {
            if (complexRate == 0) {
                PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
                VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator(polygonNum, polygonDistribution,
                        partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, normalPolygonMaxSegment);
                polygonRDD = polygonDataGen.getFeatureRDD();

            } else {
                PredefinedVectorRDD predefinedVectorRDD3 = new PredefinedVectorRDD(jsc);
                VectorRDD<Geometry> complexPolygonDataGen = predefinedVectorRDD3.getPolygonFromGenerator(polygonNum, polygonDistribution,
                        partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, complexPolygonMaxSegment);
                polygonRDD = complexPolygonDataGen.getFeatureRDD();
            }
        }

        long startTime, endTime;

        // warm up
        complexWarmUp(polygonRDD, pointRDD, 5);

        // 不切分
        startTime = System.nanoTime();
        JavaPairRDD<IFeature, IFeature> resultRDD2 = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions().set(Key.QUAD_SPLIT_THRESHOLD, -1));
        long count2 = resultRDD2.count();
        endTime = System.nanoTime();
        double execTime2 = (endTime - startTime) / 1e9;
        double throughOutput2 = count2 / execTime2;

        // 切分
        startTime = System.nanoTime();
        JavaPairRDD<IFeature, IFeature> resultRDD1 = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions().set(Key.QUAD_SPLIT_THRESHOLD, complexPolygonMaxSegment));
        long count1 = resultRDD1.count();
        endTime = System.nanoTime();
        double execTime1 = (endTime - startTime) / 1e9;
        double throughOutput1 = count1 / execTime1;


        System.out.println("复杂矢量多边形占比：" + complexRate);
        System.out.println("复杂矢量最大边数：" + complexPolygonMaxSegment);
        System.out.println("切分：count: " + count1 + " execTime1: " + execTime1 + " throughOutput1: " + throughOutput1);
        System.out.println("不切分：count: " + count2 + " execTime2: " + execTime2 + " throughOutput2: " + throughOutput2);

    }
    
    @Test
    public void ComplexSpatialJoinBatch() {
        final Distribution POINT_DISTRIBUTION = Distribution.UNIFORM;
        final Distribution POLYGON_DISTRIBUTION = Distribution.UNIFORM;
        final int SEED1 = 1229;
        final int SEED2 = 1230;
        final int TEST_ROUND = 10;
        // Define experiment parameters
        List<Integer> pointNumList;
        List<Integer> polygonNumList;
        List<Integer> partitionNumList;
        List<Double> polygonMaxSizeList;
        List<Integer> normalPolygonMaxSegmentList;
        List<Integer> complexPolygonMaxSegmentList;
        List<Double> complexRateList;
        List<Integer> roundList;

        String csvFileName = OUT_PATH + "complex/AllComplex_10Round.csv";
        roundList = new ArrayList<>();
        for (int i = 0; i < TEST_ROUND; i++) {
            roundList.add(i);
        }
        pointNumList = Arrays.asList(10000);  // Add more values as needed
        polygonNumList = Arrays.asList(1000);  // Add more values as needed
        partitionNumList = Arrays.asList(8);  // Add more values as needed
        polygonMaxSizeList = Arrays.asList(0.1);  // Add more values as needed
        normalPolygonMaxSegmentList = Arrays.asList(10);  // Add more values as needed
        complexPolygonMaxSegmentList = new ArrayList<>();
        // 4 to 1000
        for (int i = 4; i <= 1000; ) {
            complexPolygonMaxSegmentList.add(i);
            if (i <= 50) {
                i += 2;
            }
            if (i > 50 && i <= 100) {
                i += 5;
            }
            if (i > 100 && i <= 500) {
                i += 50;
            }
            if (i > 500) {
                i += 100;
            }
        }

        complexRateList = Arrays.asList(1.0);  // Add more values as needed

        // Total number of experiments
        int totalExperiments = pointNumList.size() * polygonNumList.size() * partitionNumList.size() * polygonMaxSizeList.size() * normalPolygonMaxSegmentList.size() * complexPolygonMaxSegmentList.size() * TEST_ROUND;

        // Counter for completed experiments
        int completedExperiments = 0;

        // Initialize experiment start time
        long startTime = System.currentTimeMillis();

        try (FileWriter csvWriter = new FileWriter(csvFileName)) {
            csvWriter.append("PointNum,PolygonNum,ComplexRate,PartitionNum,NormalPolygonMaxSegment,ComplexPolygonMaxSegment,Count1,ExecTime1,ThroughOutput1,Count2,ExecTime2,ThroughOutput2\n");
            // Execute experiments for each parameter set
            for (int pointNum : pointNumList) {
                for (int polygonNum : polygonNumList) {
                    for (int partitionNum : partitionNumList) {
                        for (double polygonMaxSize : polygonMaxSizeList) {
                            for (int normalPolygonMaxSegment : normalPolygonMaxSegmentList) {
                                for (int complexPolygonMaxSegment : complexPolygonMaxSegmentList) {
                                    for (double complexRate : complexRateList) {
                                        for (int round : roundList) {
                                            ComplexResult result = complexSpatialJoinTestFunc(pointNum, polygonNum, complexRate, POINT_DISTRIBUTION, POLYGON_DISTRIBUTION,
                                                    partitionNum, SEED1, SEED2, polygonMaxSize, normalPolygonMaxSegment, complexPolygonMaxSegment);
                                            csvWriter.append(String.format("%d,%d,%.2f,%d,%d,%d,%d,%.6f,%.6f,%d,%.6f,%.6f\n",
                                                    pointNum, polygonNum, complexRate, partitionNum, normalPolygonMaxSegment, complexPolygonMaxSegment,
                                                    result.count1, result.execTime1, result.throughOutput1,
                                                    result.count2, result.execTime2, result.throughOutput2));

                                            Thread.sleep(3000); // Sleep 3s to ensure the file is written
                                            // Increment counter
                                            completedExperiments++;
                                            // Print progress every 10% completion
                                            printProgress(completedExperiments, totalExperiments, startTime);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private ComplexResult complexSpatialJoinTestFunc(int pointNum, int polygonNum, double complexRate,
                                                     Distribution pointDistribution, Distribution polygonDistribution,
                                                     int partitionNum, int seed1, int seed2, double polygonMaxSize,
                                                     int normalPolygonMaxSegment, int complexPolygonMaxSegment) {
        try {
            PredefinedVectorRDD predefinedVectorRDD1 = new PredefinedVectorRDD(jsc);
            VectorRDD<Geometry> pointDataGen = predefinedVectorRDD1.getPointFromGenerator(pointNum, pointDistribution,
                    partitionNum, new ButterflyOptions().set(Key.SEED, seed1));
            JavaRDD<IFeature> pointRDD = pointDataGen.getFeatureRDD();
            JavaRDD<IFeature> polygonRDD;
            if (complexRate != 0 || complexRate != 1) {
                PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
                PredefinedVectorRDD predefinedVectorRDD3 = new PredefinedVectorRDD(jsc);
                VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator((int) (polygonNum * (1 - complexRate)), polygonDistribution,
                        partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, normalPolygonMaxSegment);
                VectorRDD<Geometry> complexPolygonDataGen = predefinedVectorRDD3.getPolygonFromGenerator((int) (polygonNum * complexRate), polygonDistribution,
                        partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, complexPolygonMaxSegment);
                JavaRDD<IFeature> simplePolygonRDD = polygonDataGen.getFeatureRDD();
                JavaRDD<IFeature> complexPolygonRDD = complexPolygonDataGen.getFeatureRDD();
                // merge simple and complex polygon
                polygonRDD = simplePolygonRDD.union(complexPolygonRDD);
            } else {
                if (complexRate == 0) {
                    PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
                    VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator(polygonNum, polygonDistribution,
                            partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, normalPolygonMaxSegment);
                    polygonRDD = polygonDataGen.getFeatureRDD();

                } else {
                    PredefinedVectorRDD predefinedVectorRDD3 = new PredefinedVectorRDD(jsc);
                    VectorRDD<Geometry> complexPolygonDataGen = predefinedVectorRDD3.getPolygonFromGenerator(polygonNum, polygonDistribution,
                            partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, complexPolygonMaxSegment);
                    polygonRDD = complexPolygonDataGen.getFeatureRDD();
                }
            }

            long startTime, endTime;

            // warm up
            complexWarmUp(polygonRDD, pointRDD, 5);

            // 不切分
            startTime = System.nanoTime();
            JavaPairRDD<IFeature, IFeature> resultRDD2 = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions().set(Key.QUAD_SPLIT_THRESHOLD, -1));
            long count2 = resultRDD2.count();
            endTime = System.nanoTime();
            double execTime2 = (endTime - startTime) / 1e9;
            double throughOutput2 = count2 / execTime2;

            // 切分
            startTime = System.nanoTime();
            JavaPairRDD<IFeature, IFeature> resultRDD1 = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions().set(Key.QUAD_SPLIT_THRESHOLD, complexPolygonMaxSegment));
            long count1 = resultRDD1.count();
            endTime = System.nanoTime();
            double execTime1 = (endTime - startTime) / 1e9;
            double throughOutput1 = count1 / execTime1;

//            // 输出到CSV文件
//            File csvFile = new File("spatial_join_results.csv");
//            try (FileWriter writer = new FileWriter(csvFile, true)) {
//                // 输出CSV表头
//                if (csvFile.length() == 0) {
//                    writer.append("ComplexRate,ComplexPolygonMaxSegment,Count1,ExecTime1,ThroughOutput1,Count2,ExecTime2,ThroughOutput2\n");
//                }
//
//                // 输出CSV数据行
//                writer.append(String.format("%.2f,%d,%d,%.6f,%.6f,%.6f,%d,%.6f,%.6f\n",
//                        complexRate, complexPolygonMaxSegment,
//                        count1, execTime1, throughOutput1,
//                        count2, execTime2, throughOutput2));
//                writer.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
            return new ComplexResult(pointNum, polygonNum, complexRate, complexPolygonMaxSegment, count1, execTime1, throughOutput1, count2, execTime2, throughOutput2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void spatialJoinTest1() {
        int pointNum = 1000;
        int polygonNum = 100;
        Distribution pointDistribution = Distribution.UNIFORM;
        Distribution polygonDistribution = Distribution.UNIFORM;
        int partitionNum = 1;
        int seed1 = 1229;
        int seed2 = 1230;
        double polygonMaxSize = 0.1;
        int polygonMaxSegment = 10;

        // 主要测试二路空间连接的正确性
        PredefinedVectorRDD predefinedVectorRDD1 = new PredefinedVectorRDD(jsc);
        PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
        VectorRDD<Geometry> pointDataGen = predefinedVectorRDD1.getPointFromGenerator(pointNum, pointDistribution,
                partitionNum, new ButterflyOptions().set(Key.SEED, seed1));
        VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator(polygonNum, polygonDistribution,
                partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, polygonMaxSegment);
        JavaRDD<IFeature> pointRDD = pointDataGen.getFeatureRDD();
        JavaRDD<IFeature> polygonRDD = polygonDataGen.getFeatureRDD();
        long startTime = System.nanoTime();
        int defaultParallelism = pointRDD.rdd().sparkContext().defaultParallelism(); // 个人电脑是8
        JavaPairRDD<IFeature, IFeature> resultRDD = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions());
        long count = resultRDD.count();
        long endTime = System.nanoTime();
        double execTime = (endTime - startTime) / 1e9;

    }

    @Test
    public void spatialSelfJoinTest1() {
        // TODO
    }

    @Test
    public void spatialMultiWayJoinTest1() {
        // TODO
    }


    @Test
    public void PointPolygonSpatialJoinTestBatch() {
        Boolean warmUp = true;
        final Distribution POINT_DISTRIBUTION = Distribution.UNIFORM;
        final Distribution POLYGON_DISTRIBUTION = Distribution.UNIFORM;
        final int SEED1 = 1229;
        final int SEED2 = 1230;

        // Define experiment parameters
        List<Integer> pointNumList;
        List<Integer> polygonNumList;
        List<Integer> partitionNumList;
        List<Double> polygonMaxSizeList;
        List<Integer> polygonMaxSegmentList;
//        String csvFileName = OUT_PATH + "/point-polygon-pointNumTest.csv";
//        pointNumList = new ArrayList<>();
//        for (int i = 0; i <= 10; i++) {
//            pointNumList.add(100000 + 100000 * i);
//        }
//        polygonNumList = Arrays.asList(10000);  // Add more values as needed
//        partitionNumList = Arrays.asList(8);  // Add more values as needed
//        polygonMaxSizeList = Arrays.asList(0.1);  // Add more values as needed
//        polygonMaxSegmentList = Arrays.asList(10);  // Add more values as needed
        String csvFileName = OUT_PATH + "twoWayJoin/point-polygon-pointNumAndPartitionTest.csv";
        pointNumList = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            pointNumList.add(100000 + 100000 * i);
        }
        polygonNumList = Arrays.asList(10000);  // Add more values as needed
        partitionNumList = Arrays.asList(1, 2, 4, 8, 16);  // Add more values as needed
        polygonMaxSizeList = Arrays.asList(0.1);  // Add more values as needed
        polygonMaxSegmentList = Arrays.asList(10);  // Add more values as needed
        // Total number of experiments
        int totalExperiments = pointNumList.size() * polygonNumList.size() * partitionNumList.size() * polygonMaxSizeList.size() * polygonMaxSegmentList.size();

        // Counter for completed experiments
        int completedExperiments = 0;

        // Initialize experiment start time
        long startTime = System.currentTimeMillis();

        // Warm-up phase
        if (warmUp) {
            joinWarmUp(5);
        }
        try (FileWriter csvWriter = new FileWriter(csvFileName)) {
            csvWriter.append("PointNum,PolygonNum,PartitionNum,PolygonMaxSize,PolygonMaxSegment,JoinResultCount,ExecTime,ThroughOutput\n");

            // Execute experiments for each parameter set
            for (int pointNum : pointNumList) {
                for (int polygonNum : polygonNumList) {
                    for (int partitionNum : partitionNumList) {
                        for (double polygonMaxSize : polygonMaxSizeList) {
                            for (int polygonMaxSegment : polygonMaxSegmentList) {
                                JoinResult result = PointPolygonSpatialJoinTestFunc(pointNum, polygonNum, POINT_DISTRIBUTION, POLYGON_DISTRIBUTION,
                                        partitionNum, SEED1, SEED2, polygonMaxSize, polygonMaxSegment);
                                // Write experiment results to CSV
                                csvWriter.append(String.format("%d,%d,%d,%.2f,%d,%d,%.6f,%.6f\n",
                                        pointNum, polygonNum, partitionNum, polygonMaxSize, polygonMaxSegment,
                                        result.joinResultCount, result.execTime, result.throughOutput));
                                Thread.sleep(3000); //sleep 3s to ensure the file is written
                                // Increment counter
                                completedExperiments++;
                                // Print progress every 10% completion
                                printProgress(completedExperiments, totalExperiments, startTime);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public JoinResult PointPolygonSpatialJoinTestFunc(int pointNum,
                                                      int polygonNum,
                                                      Distribution pointDistribution,
                                                      Distribution polygonDistribution,
                                                      int partitionNum,
                                                      int seed1,
                                                      int seed2,
                                                      double polygonMaxSize,
                                                      int polygonMaxSegment) {
        PredefinedVectorRDD predefinedVectorRDD1 = new PredefinedVectorRDD(jsc);
        PredefinedVectorRDD predefinedVectorRDD2 = new PredefinedVectorRDD(jsc);
        VectorRDD<Geometry> pointDataGen = predefinedVectorRDD1.getPointFromGenerator(pointNum, pointDistribution,
                partitionNum, new ButterflyOptions().set(Key.SEED, seed1));
        VectorRDD<Geometry> polygonDataGen = predefinedVectorRDD2.getPolygonFromGenerator(polygonNum, polygonDistribution,
                partitionNum, new ButterflyOptions().set(Key.SEED, seed2), polygonMaxSize, polygonMaxSegment);
        JavaRDD<IFeature> pointRDD = pointDataGen.getFeatureRDD();
        JavaRDD<IFeature> polygonRDD = polygonDataGen.getFeatureRDD();
        long startTime = System.nanoTime();
        JavaPairRDD<IFeature, IFeature> resultRDD = JoinHelper.spatialJoin(polygonRDD, new ButterflyOptions(), pointRDD, SJPredicate.Contains, SJAlgorithm.PBSM);
//        JavaPairRDD<IFeature, IFeature> resultRDD = JoinHelper.spatialJoin(polygonRDD, new ButterflyOptions(), pointRDD, SJPredicate.Contains);
        long count = resultRDD.count();
        long endTime = System.nanoTime();
        double execTime = (endTime - startTime) / 1e9;

        // Return experiment results
        return new JoinResult(count, execTime, count / execTime);
    }

    private void joinWarmUp(int warmUpRounds) {
        for (int round = 1; round <= warmUpRounds; round++) {
            System.out.println("Warm-up Round " + round + " started.");
            // Execute a simplified version of the experiment without recording results
            JoinResult warmUpResult = PointPolygonSpatialJoinTestFunc(1000, 1000, Distribution.UNIFORM, Distribution.UNIFORM, 4, 123, 124, 0.05, 5);
            // You can print or log the warm-up results if needed
            System.out.println("Warm-up Round " + round + " completed.");
        }
        System.out.println("Warm-up completed.");
    }

    private void complexWarmUp(JavaRDD<IFeature> polygonRDD, JavaRDD<IFeature> pointRDD, int warmUpRounds) {
        for (int i = 0; i < warmUpRounds; i++) {
            JavaPairRDD<IFeature, IFeature> warmUpResult = SpatialJoinWithSimplification.spatialJoin(polygonRDD, pointRDD, SJPredicate.Contains, null, null, new ButterflyOptions());
            warmUpResult.count();
        }
    }

    // Function to print progress information
    private void printProgress(int completed, int total, long startTime) {
        // 如果total小于等于10，或者completed未达到10%的倍数，则打印
        if (total <= 10 || completed % (total / 10) == 0) {
            double progress = ((double) completed / total) * 100;
            System.out.printf("Progress: %.2f%% (Completed: %d, Total: %d)\n", progress, completed, total);

            // Calculate estimated remaining time only if progress is greater than zero
            if (progress > 0) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                long estimatedRemainingTime = (long) ((elapsedTime / progress) * (100 - progress));
                System.out.printf("Estimated Remaining Time: %d seconds\n", estimatedRemainingTime / 1000);
            }
        }
    }


    // Class to hold join results
    private static class JoinResult {
        long joinResultCount;
        double execTime;
        double throughOutput;

        public JoinResult(long joinResultCount, double execTime, double throughOutput) {
            this.joinResultCount = joinResultCount;
            this.execTime = execTime;
            this.throughOutput = throughOutput;
        }
    }

    // Class to hold complex results
    // PointNum,PolygonNum,ComplexRate,ComplexPolygonMaxSegment,Count1,ExecTime1,ThroughOutput1,Count2,ExecTime2,ThroughOutput2
    private static class ComplexResult {
        int pointNum;
        int polygonNum;
        double complexRate;
        int complexPolygonMaxSegment;
        long count1;
        double execTime1;
        double throughOutput1;
        long count2;
        double execTime2;
        double throughOutput2;

        public ComplexResult(int pointNum, int polygonNum, double complexRate, int complexPolygonMaxSegment,
                             long count1, double execTime1, double throughOutput1,
                             long count2, double execTime2, double throughOutput2) {
            this.pointNum = pointNum;
            this.polygonNum = polygonNum;
            this.complexRate = complexRate;
            this.complexPolygonMaxSegment = complexPolygonMaxSegment;
            this.count1 = count1;
            this.execTime1 = execTime1;
            this.throughOutput1 = throughOutput1;
            this.count2 = count2;
            this.execTime2 = execTime2;
            this.throughOutput2 = throughOutput2;
        }
    }
}
