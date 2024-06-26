package cn.edu.whu.lynn.davinci;

import cn.edu.whu.lynn.davinci.CommonVisualizationHelper;
import cn.edu.whu.lynn.davinci.GeometricPlotter;
import cn.edu.whu.lynn.davinci.SingleLevelPlotHelper;
import cn.edu.whu.lynn.io.CSVEnvelopeDecoderJTS;
import cn.edu.whu.lynn.io.CSVFeatureReader;
import cn.edu.whu.lynn.io.SpatialFileRDD;
import cn.edu.whu.lynn.test.JavaSparkTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

public class SingleLevelPlotHelperTest extends JavaSparkTest {

  public void testPlotLocalKeepRatio() throws IOException {
    Path inPath = new Path(scratchPath(), "in.rect");
    copyResource("/test.rect", new File(inPath.toString()));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.InputFormat(), "envelopek(2)");
    conf.set(CSVFeatureReader.FieldSeparator, ",");
    conf.set("mbr", "-180.0,-45.0,180.0,45.0");
    SingleLevelPlotHelper.plotLocal(inPath, dos, GeometricPlotter.class, 1000, 1000, true, conf);
    dos.close();

    BufferedImage i = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(1000, i.getWidth());
    assertEquals(250, i.getHeight());
  }

  public void testPlotLocalMercatorProjection() throws IOException {
    Path inPath = new Path(scratchPath(), "in.rect");
    copyResource("/test.rect", new File(inPath.toString()));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.InputFormat(), "envelopek(2)");
    conf.set(CSVFeatureReader.FieldSeparator, ",");
    conf.setBoolean(CommonVisualizationHelper.UseMercatorProjection, true);
    conf.set("mbr", "-180.0,-45.0,180.0,45.0");
    SingleLevelPlotHelper.plotLocal(inPath, dos, GeometricPlotter.class, 1000, 1000, true, conf);
    dos.close();

    BufferedImage i = ImageIO.read(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(1000.0, (double) i.getWidth(), 1.0);
    assertEquals(280.0, (double)i.getHeight(), 1.0);
  }

  public void testPlotLocalFilterWithBuffer() throws IOException {
    Path inPath = new Path(scratchPath(), "in.rect");
    copyResource("/test.rect", new File(inPath.toString()));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Configuration conf = new Configuration();
    conf.set(SpatialFileRDD.InputFormat(), "envelopek(2)");
    conf.set(CSVFeatureReader.FieldSeparator, ",");
    conf.setInt(GeometricPlotter.PointSize, 10);
    conf.set("mbr", "-180.0,-45.0,180.0,45.0");
    SingleLevelPlotHelper.plotLocal(inPath, dos, GeometricPlotter.class, 360, 90, true, conf);
    dos.close();

    Envelope filterMBR = CSVEnvelopeDecoderJTS.instance.apply(conf.get(SpatialFileRDD.FilterMBR()), null);
    assertEquals(new Envelope(-190.0,190.0,-55.0,55.0), filterMBR);
  }
}