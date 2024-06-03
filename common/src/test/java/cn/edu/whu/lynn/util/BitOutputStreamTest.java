package cn.edu.whu.lynn.util;

import cn.edu.whu.lynn.test.JavaSparkTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BitOutputStreamTest extends JavaSparkTest {

  public void testWriteSomeBits() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BitOutputStream bos = new BitOutputStream(baos);

    bos.write(0x3, 6);
    bos.write(0x2, 4);
    bos.write(0x5, 3);
    bos.write(0xf, 4);
    bos.write(0x23, 7);

    bos.close();

    byte[] bytes = baos.toByteArray();
    assertArrayEquals(new byte[] {0x0c, (byte) 0xAF, (byte) 0xA3}, bytes);
  }
}