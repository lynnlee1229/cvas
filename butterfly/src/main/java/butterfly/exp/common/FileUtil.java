package butterfly.exp.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lynn Lee
 * @date 2024/3/5
 **/
public class FileUtil {
    private static final Logger LOGGER = Logger.getLogger(FileUtil.class);

    public static List<String> getFileNames(String fatherPath) {
        List<String> files = new ArrayList<>();
        File fatherFile = new File(fatherPath);
        File[] fileList = fatherFile.listFiles();

        for (int i = 0; i < (fileList != null ? fileList.length : 0); ++i) {
            if (fileList[i].isFile()) {
                files.add(fileList[i].toString());
            }
        }
        return files;
    }


    /**
     * 按行读取CSV文件
     *
     * @param filePath 文件路径（包括文件名）
     * @param hasTitle 是否有标题行
     * @return
     */
    public static List<String> readCSV(String filePath, boolean hasTitle) {
        List<String> data = new ArrayList<>();
        String line = null;
        try {
            BufferedReader bufferedReader =
                    new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "utf-8"));
            if (hasTitle) {
                //第一行信息，为标题信息，不返回
                line = bufferedReader.readLine();
//        data.add(line);
                System.out.println("标题行：" + line);
            }
            while ((line = bufferedReader.readLine()) != null) {
                //数据行
                data.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    /**
     * 一次性读取文本文件
     *
     * @param fileName 文件名
     * @return
     */
    public static String readFileToString(String fileName) {
        String enCoding = "UTF-8";
        File file = new File(fileName);
        Long fileLength = file.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(fileContent);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(fileContent, enCoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String readFileToString(InputStream is){
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder stringBuilder = new StringBuilder();
        String s = "";
        while (true) {
            try {
                if (!((s = br.readLine()) != null)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            stringBuilder.append(s);
        }
        return stringBuilder.toString();
    }

    public static void writeStringToFile(String fileName, String content) throws IOException {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 读取文件
     *
     * @param filePath 文件路径
     * @return 文件字符串
     */
    public static String readFully(String fsDefaultName, String filePath) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", fsDefaultName);
        Path path = new Path(filePath);
        try (FileSystem fs = FileSystem.get(URI.create(filePath), conf);
             FSDataInputStream in = fs.open(path)) {
            FileStatus stat = fs.getFileStatus(path);
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            in.readFully(0, buffer);
            return new String(buffer);
        } catch (IOException e) {
            LOGGER.error(e.getMessage() + "/nFailed to read file from : " + filePath);
            e.printStackTrace();
            return null;
        }
    }
}
