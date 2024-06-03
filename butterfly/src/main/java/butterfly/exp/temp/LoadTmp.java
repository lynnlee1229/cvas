package butterfly.exp.temp;

import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import butterfly.core.spatialrdd.ButterflySparkContext;
import butterfly.exp.common.FileUtil;
import butterfly.exp.common.ExpUtil;
import cn.edu.whu.lynn.geolite.IFeature;
import org.apache.spark.api.java.JavaRDD;

import java.io.InputStream;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/5
 **/
public class LoadTmp {
    public static void main(String[] args) {
        String fileStr;
        if (args.length > 1) {
            String fs = args[0];
            String filePath = args[1];
            fileStr = FileUtil.readFully(fs, filePath);
        } else if (args.length == 1) {
            String confPath = args[0];
            fileStr = FileUtil.readFileToString(confPath);
        } else {
            InputStream resourceAsStream = LoadTmp.class.getClassLoader()
                    .getResourceAsStream("temp/load/WKTTmp.json");
            fileStr = FileUtil.readFileToString(resourceAsStream);
        }
        ButterflyConfiguration config = ButterflyConfiguration.from(fileStr);
        ButterflySparkContext bsc = ExpUtil.createButterflySparkContext(config);
        List<JavaRDD<IFeature>> rddList = ExpUtil.getRddList(bsc, config);
        // Rest of your code
        for (JavaRDD<IFeature> rdd : rddList) {
            System.out.println(rdd.count());
        }
    }

}
