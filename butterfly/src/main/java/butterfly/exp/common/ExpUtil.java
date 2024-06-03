package butterfly.exp.common;

import butterfly.core.algrithm.buffer.BufferHelper;
import butterfly.core.algrithm.join.JoinHelper;
import butterfly.core.common.Key;
import butterfly.core.enums.Distribution;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.exp.batch.JoinBatchTemp;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import cn.edu.whu.lynn.core.SJAlgorithm;
import cn.edu.whu.lynn.core.SJPredicate;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Option;

import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Lynn Lee
 * @date 2024/3/5
 **/
public class ExpUtil implements Serializable {
    public static ButterflyConfiguration parseConfig(String[] args, String defaultPath) {
        String fileStr;
        // 解析文件参数
        fileStr = parseFileStr(args, defaultPath);
        ButterflyConfiguration config = ButterflyConfiguration.from(fileStr);
        String joinPairType = config.getString(Key.JOIN_PAIR_TYPE, "Point-Polygon");
        if (Objects.equals(joinPairType, "Polygon-Polygon")) {
            config.set(Key.RDDS_KEY, config.getListConfiguration(Key.RDDS_KEY_BAK));
        }
        // 解析key%value参数
        Map<String, String> paramMap = parseKeyValueParams(args);
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            // 保证value和原value类型一致
            // 获取原始配置中对应参数的类型
            Object originalValue = config.get(entry.getKey());
            // 根据原始值的类型转换参数值的类型
            Object convertedValue = convertToOriginalType(originalValue, entry.getValue());
            // 设置参数值
            config.set(entry.getKey(), convertedValue);
        }
        config.set(Key.PARAM_MAP, paramMap);
        return config;
    }

    public static ButterflyConfiguration parseConfig(String[] args) {
        return parseConfig(args, "conftemp/WKT2JoinLocalTemp.json");
    }

    public static String parseFileStr(String[] args, String defaultPath) {
        String fileStr;
        if (args.length > 0) {
            // 当提供了参数时
            String firstArg = args[0];
            if (firstArg.contains("hdfs")) {
                // 参数中含有hdfs，解析fs和filePath并调用readFully
                String[] parts = extractFsAndFilePath(firstArg);
                if (parts.length != 2) {
                    System.out.println("Error: Invalid hdfs file path format.");
                    System.exit(1);
                }
                fileStr = FileUtil.readFully(parts[0], parts[1]);
            } else {
                // 调用readFileToString，参数为temp文件路径
                fileStr = FileUtil.readFileToString(firstArg);
            }
        } else {
            // 如果未提供参数，则使用资源中的默认文件
            InputStream resourceAsStream = JoinBatchTemp.class.getClassLoader()
                    .getResourceAsStream(defaultPath);
            fileStr = FileUtil.readFileToString(resourceAsStream);
        }
        return fileStr;
    }

    public static String parseFileStr(String[] args) {
        return parseFileStr(args, "conftemp/WKT2JoinLocalTemp.json");
    }

    public static String[] extractFsAndFilePath(String hdfsPath) {
        Pattern pattern = Pattern.compile("hdfs://([^/]+)/(.+)");
        Matcher matcher = pattern.matcher(hdfsPath);
        if (matcher.find()) {
            String fs = "hdfs://" + matcher.group(1);
            String filePath = "/" + matcher.group(2);
            return new String[]{fs, filePath};
        } else {
            return new String[]{};
        }
    }

    public static Object convertToOriginalType(Object originalValue, String newValue) {
        // 根据原始值的类型转换新值的类型
        if (originalValue instanceof Integer) {
            return Integer.parseInt(newValue);
        } else if (originalValue instanceof Long) {
            return Long.parseLong(newValue);
        } else if (originalValue instanceof Double) {
            return Double.parseDouble(newValue);
        } else if (originalValue instanceof Boolean) {
            return Boolean.parseBoolean(newValue);
        } else {
            // 其他类型的值保持不变
            return newValue;
        }
    }

    public static Map<String, String> parseKeyValueParams(String[] args) {
        Map<String, String> paramMap = new HashMap<>();
        for (String arg : args) {
            if (arg.contains("%")) {
                String[] keyValue = arg.split("%");
                if (keyValue.length != 2) {
                    System.out.println("Error: Invalid key:value format.");
                    System.exit(1);
                }
                paramMap.put(keyValue[0], keyValue[1]);
            }
        }
        return paramMap;
    }

    public static List<JavaRDD<IFeature>> getRddList(ButterflySparkContext bsc, ButterflyConfiguration config) {
        List<JavaRDD<IFeature>> rddList = new ArrayList<>();
        String joinPairType = config.getString(Key.JOIN_PAIR_TYPE, "Point-Polygon");
        List<ButterflyConfiguration> configList = null;
        configList = config.getListConfiguration(Key.RDDS_KEY);
//        if (Objects.equals(joinPairType, "Polygon-Polygon")) {
//            configList = config.getListConfiguration(Key.RDDS_KEY_BAK);
//        } else {
//            configList = config.getListConfiguration(Key.RDDS_KEY);
//        }
        String fatherPath = config.getString(Key.IN_FATHER_PATH_KEY, null);
        for (ButterflyConfiguration configItem : configList) {
            if (fatherPath != null && configItem.getString(Key.PATH_KEY, null) == null) {
                configItem.set(Key.PATH_KEY, fatherPath + configItem.getString(Key.RELATIVE_PATH_KEY));
            }
            JavaRDD<IFeature> rdd = createRDDFromConfig(bsc, configItem);
            rddList.add(rdd);
        }
        return rddList;
    }


    public static JavaSparkContext createJavaSparkContext(ButterflyConfiguration config) {
        String master = config.getString(Key.MASTER, "local[*]");
        String appName = config.getString(Key.APP_NAME, "defaultAppName");
        return new JavaSparkContext(master, appName);
    }

    public static ButterflySparkContext createButterflySparkContext(ButterflyConfiguration config) {
        String master = config.getString(Key.MASTER, "local[*]");
        String appName = config.getString(Key.APP_NAME, "defaultAppName");
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("fs.permissions.umask-mode", "022");
        sparkConf.set("spark.hadoop.fs.hdfs.impl.disable.cache", "true");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        sparkConf.set("spark.kryoserializer.buffer", "64m");
        sparkConf.set("spark.default.parallelism", config.getString(Key.SPARK_DEFAULT_PARALLELISM, "8"));
        sparkConf.registerKryoClasses(
                new Class[]{IFeature.class, STRtree.class, SimpleFeatureTypeImpl.class, SimpleFeatureBuilder.class});
        sparkConf.setMaster(master);
        sparkConf.setAppName(appName);
        return new ButterflySparkContext(sparkConf);
    }

    public static JavaRDD<IFeature> createRDDFromConfig(ButterflySparkContext bsc, ButterflyConfiguration configItem) {
        String readerType = configItem.getString(Key.READER_TYPE);
        JavaRDD<IFeature> rdd = null;

        switch (readerType.toLowerCase()) {
            case "wkt":
                rdd = bsc.readWKTFile(
                        configItem.getString(Key.PATH_KEY),
                        configItem.get(Key.WKT_COLUMN),
                        configItem.getString(Key.DELIMITER, ","),
                        configItem.getBoolean(Key.SKIP_HEADER, false)
                );
                break;
            case "csvpoint":
                rdd = bsc.readCSVPoint(
                        configItem.getString(Key.PATH_KEY),
                        configItem.get(Key.X_COLUMN),
                        configItem.get(Key.Y_COLUMN),
                        configItem.getString(Key.DELIMITER, ","),
                        configItem.getBoolean(Key.SKIP_HEADER, false)
                );
                break;
            case "shapefile":
                rdd = bsc.shapefile(configItem.getString(Key.PATH_KEY));
                break;
            case "genpolygon":
                Distribution polygonDistribution = Distribution.getDistributionType(configItem.getString(Key.DISTRIBUTION, "uniform"));
                long polygonCardinality = configItem.getLong(Key.CARDINALITY, 0L);
                Integer polygonNumPartitions = configItem.getInt(Key.NUM_PARTITIONS, bsc.sc().defaultParallelism());
                ButterflyOptions polygonOpts = parseButterflyOptions(configItem.getConfiguration(Key.OPTS));
                Double polygonMaxSize = configItem.getDouble(Key.MAX_SIZE, 0.1);
                Integer polygonNumSegments = configItem.getInt(Key.NUM_SEGMENTS, 4);
                Map<String, Object> complexityParam = configItem.getMap(Key.COMPLEXTIY_PARAM);
                if (complexityParam != null) {
                    Double ratio = Double.valueOf(complexityParam.get("ratio").toString());
                    long complexCardinality = (long) (polygonCardinality * ratio);
                    complexCardinality = complexCardinality < 1 ? 1 : complexCardinality;
                    long baseCardinality = polygonCardinality - complexCardinality;
                    Double complexPolygonMaxSize = Double.valueOf(complexityParam.get("maxSize").toString());
                    Integer complexPolygonNumSegments = Integer.valueOf(complexityParam.get("numSegments").toString());
                    JavaRDD<IFeature> complexRDD = bsc.generatePolygonData(polygonDistribution, complexCardinality, polygonNumPartitions, polygonOpts, complexPolygonMaxSize, complexPolygonNumSegments);
                    List<IFeature> complexList = complexRDD.collect();
                    // baseRDD移除complexCardinality个元素
                    JavaRDD<IFeature> baseRDD = bsc.generatePolygonData(polygonDistribution, baseCardinality, polygonNumPartitions, polygonOpts, polygonMaxSize, polygonNumSegments);
                    // 把complexRDD中的元素添加到baseRDD中
                    rdd = baseRDD.union(bsc.parallelize(complexList, polygonNumPartitions));
                    // 打印出rdd中点数大于100的polygon
//                    rdd.filter(p -> p.getGeometry().getNumPoints() > 100).collect().forEach(p -> System.out.println(p.getGeometry().getNumPoints() + " : " + p.getGeometry().getGeometryType()));
                } else {
                    rdd = bsc.generatePolygonData(polygonDistribution, polygonCardinality, polygonNumPartitions, polygonOpts, polygonMaxSize, polygonNumSegments);
                }
                break;
            case "genpoint":
                Distribution pointDistribution = Distribution.getDistributionType(configItem.getString(Key.DISTRIBUTION, "uniform"));
                long pointCardinality = configItem.getLong(Key.CARDINALITY, 0L);
                Integer pointNumPartitions = configItem.getInt(Key.NUM_PARTITIONS, bsc.sc().defaultParallelism());
                ButterflyOptions pointOpts = parseButterflyOptions(configItem.getConfiguration(Key.OPTS));
                rdd = bsc.generatePointData(pointDistribution, pointCardinality, pointNumPartitions, pointOpts);
                break;
            default:
                // Handle unknown reader type or provide a default case
                break;
        }

        return rdd;
    }

    public static JavaPairRDD<IFeature, IFeature> getJoinResult(List<JavaRDD<IFeature>> rddList, ButterflyConfiguration joinParam, LongAccumulator mbrCount) {
        // perform join
        String joinType = joinParam.getString(Key.JOIN_TYPE_KEY);
        JavaPairRDD<IFeature, IFeature> joinResult = null;
        ButterflyOptions opts = ExpUtil.parseButterflyOptions(joinParam.getConfiguration(Key.OPTS));
        SJPredicate predicate = SJPredicate.getSJPredicate(joinParam.getString(Key.PREDICATE, null));
        SJAlgorithm algorithm = SJAlgorithm.getSJAlgorithm(joinParam.getString(Key.ALGORITHM, null));

        switch (joinType.toLowerCase()) {
            case "twowayjoin":
                if (rddList.size() != 2) {
                    System.out.println("2路连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                JavaRDD<IFeature> driverRDD = rddList.get(0);
                // 被驱动数据集
                JavaRDD<IFeature> drivenRDD = rddList.get(1);
                joinResult = JoinHelper.spatialJoin(driverRDD, opts, drivenRDD, predicate, algorithm, mbrCount);
                break;
            case "selfjoin":
                if (rddList.size() != 1) {
                    System.out.println("自连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                JavaRDD<IFeature> targetRDD = rddList.get(0);
                joinResult = JoinHelper.spatialSelfJoin(targetRDD, opts, predicate);
                break;
            case "multiwayjoin":
                // TODO 多路连接
                break;
            default:
                System.out.println("暂不支持的连接类型，请调整配置文件后重试！");
                return null;
        }
        return joinResult;
    }

    public static JavaPairRDD<IFeature, IFeature> getJoinResultWithMetric(List<JavaRDD<IFeature>> rddList, ButterflyConfiguration joinParam, MetricsAccumulator metricsAccumulator) {
        // perform join
        String joinType = joinParam.getString(Key.JOIN_TYPE_KEY);
        JavaPairRDD<IFeature, IFeature> joinResult = null;
        ButterflyOptions opts = ExpUtil.parseButterflyOptions(joinParam.getConfiguration(Key.OPTS));
        SJPredicate predicate = SJPredicate.getSJPredicate(joinParam.getString(Key.PREDICATE, null));
        SJAlgorithm algorithm = SJAlgorithm.getSJAlgorithm(opts.getString(Key.ALGORITHM, null));
        switch (joinType.toLowerCase()) {
            case "twowayjoin":
                if (rddList.size() < 2) {
                    System.out.println("2路连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                JavaRDD<IFeature> driverRDD = rddList.get(0);
                // 被驱动数据集
                JavaRDD<IFeature> drivenRDD = rddList.get(1);
                joinResult = JoinHelper.spatialJoinWithMetric(driverRDD, opts, drivenRDD, predicate, algorithm, metricsAccumulator);
                break;
            case "selfjoin":
                if (rddList.size() < 1) {
                    System.out.println("自连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                JavaRDD<IFeature> targetRDD = rddList.get(0);
                joinResult = JoinHelper.spatialSelfJoinWithMetric(targetRDD, opts, metricsAccumulator, predicate);
                break;
            case "selfjoinbl":
                if (rddList.size() < 1) {
                    System.out.println("自连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                joinResult = JoinHelper.spatialSelfJoinBaseLine(rddList.get(0), opts, metricsAccumulator, predicate);
                break;
            case "multiwayjoin":
                // TODO 多路连接
                break;
            case "twowayjoinsd":
                if (rddList.size() < 2) {
                    System.out.println("2路连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                joinResult = JoinHelper.spatialJoinSedona(rddList.get(0), opts, rddList.get(1), predicate, algorithm, metricsAccumulator);
                break;
            case "selfjoinsd":
                if (rddList.size() < 1) {
                    System.out.println("自连接所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                joinResult = JoinHelper.spatialJoinSedona(rddList.get(0), opts, rddList.get(0), predicate, algorithm, metricsAccumulator);
                break;
            default:
                System.out.println("暂不支持的连接类型，请调整配置文件后重试！");
                return null;
        }
        return joinResult;
    }

    public static JavaRDD<IFeature> getAlgorithmResult(List<JavaRDD<IFeature>> rddList, ButterflyConfiguration algParam, MetricsAccumulator metricsAccumulator) {
        // perform algorithm
        String algType = algParam.getString(Key.ALG_TYPE_KEY);
        JavaRDD<IFeature> algResult = null;
        ButterflyOptions opts = ExpUtil.parseButterflyOptions(algParam.getConfiguration(Key.OPTS));
        switch (algType.toLowerCase()) {
            case "bufferv1":
                if (rddList.size() != 1) {
                    System.out.println("暂不支持多个数据集同时生成缓冲区，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                opts.set("keepLeft", true);
                algResult = BufferHelper.bufferV1(rddList.get(0), metricsAccumulator, opts);
                break;
            case "bufferv2":
                if (rddList.size() != 1) {
                    System.out.println("暂不支持多个数据集同时生成缓冲区，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                algResult = BufferHelper.bufferV2(rddList.get(0), metricsAccumulator, opts);
                break;
            case "bufferbaseline":
                if (rddList.size() != 1) {
                    System.out.println("暂不支持多个数据集同时生成缓冲区，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                algResult = BufferHelper.bufferBaseline(rddList.get(0), metricsAccumulator, opts);
                break;
            case "overlap":
                if (rddList.size() < 2) {
                    System.out.println("叠置所需数据集数量不正确，请调整配置文件后重试！");
                    return null;
                }
                // 驱动数据集
                // TODO 增加叠置算法支持
                break;
            default:
                System.out.println("暂不支持的分析算法类型，请调整配置文件后重试！");
                return null;
        }
        return algResult;
    }

    public static ButterflyOptions parseButterflyOptions(ButterflyConfiguration config) {
        // 在这里实现解析 ButterflyOptions 的逻辑
        // 根据配置项构造 ButterflyOptions 对象并返回
        if (config == null) {
            return new ButterflyOptions();
        }
        Set<String> keys = config.getKeys();
        ButterflyOptions opts = new ButterflyOptions();
        for (String key : keys) {
            opts.set(key, config.getString(key));
        }
        Option<String> partitioner = opts.get(Key.PARTITIONER);
        if (!partitioner.isEmpty()) {
            opts.set(Key.PBSM_PARTITIONER, partitioner.get());
        }
        return opts;
    }
}
