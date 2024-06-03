package butterfly.example.diploma;

import butterfly.core.spatialrdd.VectorRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Lynn Lee
 * @date 2023/12/8
 **/
public class geoRDDTest {
    public void fromBeast() throws Exception {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "beastTest");
        VectorRDD<Geometry> vectorRDD = new VectorRDD<>(jsc);

//        JavaRDD<IFeature> featureJavaRDD = javaSpatialSparkContext.readWKTFile(TestConstant.SHENZHEN_LANDUSE, "geometry", ',', true);
//        VectorRDD geomRDD = new VectorRDD(featureJavaRDD);
//        JavaRDD<Geometry> geometryJavaRDD = geomRDD.getVectorJavaRDD();
//        geometryJavaRDD.filter(g -> !(g instanceof MultiPolygon)).count();
    }
}
