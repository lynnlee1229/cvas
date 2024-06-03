package butterfly.exp.common;

/**
 * @author Lynn Lee
 * @date 2024/3/10
 **/

import butterfly.core.common.Key;
import cn.edu.whu.lynn.conf.ButterflyConfiguration;
import cn.edu.whu.lynn.core.MetricsAccumulator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class HDFSUtil {
    public static void writeToCSV(ButterflyConfiguration butterflyConfiguration, MetricsAccumulator metricsAccumulator) {
        try {
            String fsUrl = butterflyConfiguration.getString(Key.OUT_FATHER_PATH_KEY, "hdfs://localhost:9000");
            String filePath = butterflyConfiguration.getString(Key.OUT_FILE_PATH_KEY, "/");
            String fileName = butterflyConfiguration.getString(Key.OUT_FILE_NAME_KEY, null);
            if (fileName != null) {
                filePath = filePath + "/" + fileName;
            } else {
                filePath = filePath + "/" + butterflyConfiguration.getString(Key.APP_NAME, "default") + ".csv";
            }
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", fsUrl);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","false");
//            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","DEFAULT");
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            String csvContent;
            if (!fs.exists(path)) {
                // 如果文件不存在，则生成包含header的CSV内容
                System.out.println("Creating new file: " + filePath);
                csvContent = metricsAccumulator.toCSV(true);
            } else {
                // 如果文件已经存在，则生成不包含header的CSV内容
                System.out.println("Appending to file: " + filePath);
                csvContent = metricsAccumulator.toCSV(false);
            }

            // 写入CSV内容到文件
            OutputStream os = fs.exists(path) ? fs.append(path) : fs.create(path);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os));
            br.write(csvContent);
            br.newLine();
            br.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

