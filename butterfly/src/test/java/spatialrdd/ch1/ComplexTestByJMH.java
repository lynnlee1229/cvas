package spatialrdd.ch1;

import butterfly.core.enums.Distribution;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.core.utils.GeoUtils;
import cn.edu.whu.lynn.common.ButterflyOptions;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Lynn Lee
 * @date 2024/3/2
 **/
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@Threads(1)
public class ComplexTestByJMH {
    //    public List<IFeature> complexPolygons = null;
//    public List<Geometry> complexGeometry = null;
//
//    {
//        FATHER_PATH = "outs/ch1/complex";
//        int PolygonSegmentNum = 10000;
//        JavaRDD<IFeature> complexPolygonRDD = genNonPointData(null, 1, 1, null, PolygonSegmentNum, null);
//        complexGeometry = complexPolygonRDD.map(f -> f.getGeometry()).collect();
//    }
    public static ButterflySparkContext bsc = new ButterflySparkContext("local[*]", "complexTest");
    @Param(value = {"200","300","400","500","600","700","800","900",
            "1000", "2000", "3000","4000","5000","6000","7000","8000","9000","10000"})
    int PolygonSegmentNum;


    public JavaRDD<IFeature> genNonPointData(Distribution distribution, Integer nonPointNum, Integer numPartitions, ButterflyOptions opts, Integer polygonSegmentNum, Double polygonSize) {
        return bsc.generatePolygonData(distribution, nonPointNum, numPartitions, opts, polygonSize, polygonSegmentNum);
    }

    @Benchmark
    public void nonSparkComplexTest1() {
//        JavaRDD<IFeature> complexPolygonRDD = genNonPointData(Distribution.UNIFORM, 1, 1, new ButterflyOptions(), PolygonSegmentNum, 0.1);
//        Geometry geometry = complexPolygonRDD.map(f -> f.getGeometry()).first();
//        GeoUtils.quadSplit(geometry, 100);
        JavaRDD<IFeature> complexPolygonRDD = genNonPointData(Distribution.UNIFORM, 1000, 8, new ButterflyOptions(), PolygonSegmentNum, 0.1);
        List<Geometry> geometryList = complexPolygonRDD.map(f -> f.getGeometry()).collect();
        for (Geometry geometry : geometryList) {
            GeoUtils.quadSplit(geometry, 100);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder()
                .include(ComplexTestByJMH.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .output("/Users/lynnlee/Code/review/beast/butterfly/outs/ch1/complex_Test_2.json")
                .build();
        new Runner(opts).run();

    }

}
