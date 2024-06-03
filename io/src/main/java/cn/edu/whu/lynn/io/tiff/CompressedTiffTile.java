/*
 * Copyright 2021 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.whu.lynn.io.tiff;

import com.esotericsoftware.kryo.DefaultSerializer;
import cn.edu.whu.lynn.util.LZWCodec;
import cn.edu.whu.lynn.util.MathUtil;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * A tile from a TIFF file that might not be decompressed. The tile is decompressed lazily when the first pixel value
 * is requested. This allows the tile to be efficiently kept in memory and serialized without decompressing it.
 * This class is designed to
 * be standalone and is not associated with an open TIFF file. This makes it possible to serialize this class over
 * network and read the pixel values on another machine.
 */
@DefaultSerializer(TiffTileSerializer.class)
public class CompressedTiffTile extends AbstractTiffTile {

  /**The data in the tile. This is either the compressed or decompressed data based on the {@link #compressionScheme}*/
  protected byte[] tileData;

  /**The compression scheme of the data*/
  protected int compressionScheme;

  /**The predictor of the pixel values, e.g., diff*/
  protected int predictor;

  /**If JPEG compression is used and JPEGTable is provided.*/
  protected byte[] jpegTable;

  public CompressedTiffTile(byte[] tileData, int compressionScheme, int predictor,
                            int[] bitsPerSample, int[] sampleFormats, int bitsPerPixel,
                            int i1, int j1, int i2, int j2, int planarConfiguration, boolean littleEndian) {
    super(i1, j1, i2, j2, bitsPerSample, sampleFormats, bitsPerPixel, planarConfiguration, littleEndian);
    this.tileData = tileData;
    this.compressionScheme = compressionScheme;
    this.predictor = predictor;
  }

  public void setJpegTable(byte[] table) {
    this.jpegTable = table;
  }

  /**
   * Detects if the given compression scheme is supported for decoding.
   * @param compressionScheme the compression scheme as defined in TIFF file specification
   * @return {@code true} if it is supported, {@code false} otherwise.
   */
  static boolean isCompressionSupported(int compressionScheme) {
    switch (compressionScheme) {
      case TiffConstants.COMPRESSION_NONE:
      case TiffConstants.COMPRESSION_LZW:
      case TiffConstants.COMPRESSION_JPEG2:
      case TiffConstants.COMPRESSION_DEFLATE:
      case TiffConstants.COMPRESSION_PACKBITS:
        return true;
      default:
        return false;
    }
  }

  /**
   * Return the decompressed array of bytes with the tile data. If the tile is not loaded, it lazily loads it.
   * The loaded data is not serialized to keep the serialization cost low.
   * @return an array of decompressed tile data
   */
  public byte[] getTileData() {
    if (compressionScheme != TiffConstants.COMPRESSION_NONE) {
      // Need to decompress the data first
      switch (compressionScheme) {
        case TiffConstants.COMPRESSION_LZW:
          tileData = LZWCodec.decode(tileData,
              getTileWidth() * getTileHeight() * bitsPerPixel / 8, true);
          break;
        case TiffConstants.COMPRESSION_DEFLATE:
          try {
            Inflater inflater = new Inflater();
            inflater.setInput(tileData);
            tileData = new byte[getTileWidth() * getTileHeight() * bitsPerPixel / 8];
            int decompressionLength = inflater.inflate(tileData);
            assert decompressionLength == tileData.length :
                String.format("Mismatching length between. Decompressed length %d != expected length %d",
                    decompressionLength, tileData.length);
          } catch (DataFormatException e) {
            throw new RuntimeException("Error inflating TIFF tile", e);
          }
          break;
        case TiffConstants.COMPRESSION_JPEG2:
          decodeJPEG();
          break;
        case TiffConstants.COMPRESSION_PACKBITS:
          try {
            PackBitsInputStream pbis = new PackBitsInputStream(tileData);
            tileData = new byte[(getTileWidth() * getTileHeight() * bitsPerPixel + 7) / 8];
            int decompressionLength = pbis.read(tileData);
            assert decompressionLength == tileData.length :
                String.format("Mismatching length. Decompressed length %d != expected length %d",
                    decompressionLength, tileData.length);
          } catch (IOException e) {
            throw new RuntimeException("Unexpected error", e);
          }
          break;
        default:
          throw new RuntimeException(String.format("Unsupported compression scheme %d", compressionScheme));
      }
      compressionScheme = TiffConstants.COMPRESSION_NONE;
    }
    if (predictor == 2) {
      // Apply the differencing algorithm
      // Special case when all components are 8-bits which make the differencing simpler
      int minBitsPerSample = bitsPerSample[0];
      int maxBitsPerSample = bitsPerSample[0];
      for (int iSample = 1; iSample < getNumSamples(); iSample++) {
        minBitsPerSample = Math.min(minBitsPerSample, bitsPerSample[iSample]);
        maxBitsPerSample = Math.max(maxBitsPerSample, bitsPerSample[iSample]);
      }
      if (minBitsPerSample == 8 && maxBitsPerSample == 8) {
        // This is the easiest case to handle with an efficient algorithm
        for (int jPixel = 0; jPixel < getTileHeight(); jPixel++) {
          int offset = (jPixel * getTileWidth() + 1) * getNumSamples();
          int endOffset = ((jPixel + 1) * getTileWidth()) * getNumSamples();
          while (offset < endOffset) {
            tileData[offset] += tileData[offset - getNumSamples()];
            offset++;
          }
        }
      } else if (minBitsPerSample == 16 && maxBitsPerSample == 16) {
        // Values are short integers
        int numSamples = getNumSamples();
        short[] previousPixel = new short[numSamples];
        for (int jPixel = 0; jPixel < getTileHeight(); jPixel++) {
          int offset = (jPixel * getTileWidth()) * numSamples * 2;
          int endOffset = ((jPixel + 1) * getTileWidth()) * numSamples * 2;
          for (int iSample = 0; iSample < numSamples; iSample++) {
            previousPixel[iSample] = (short) ((tileData[offset] & 0xff) | ((tileData[offset + 1] & 0xff) << 8));
            offset += 2;
          }
          while (offset < endOffset) {
            for (int iSample = 0; iSample < numSamples; iSample++) {
              short diff = (short) ((tileData[offset] & 0xff) | ((tileData[offset + 1] & 0xff) << 8));
              previousPixel[iSample] += diff;
              tileData[offset] = (byte) previousPixel[iSample];
              tileData[offset + 1] = (byte) (previousPixel[iSample] >> 8);
              offset += 2;
            }
          }
        }
      } else {
        if (planarConfiguration == TiffConstants.PlanarFormat)
          throw new RuntimeException("Does not yet support PlanarFormat");
        // General case could be less efficient
        int numSamples = getNumSamples();
        int[] previousPixel = new int[numSamples];
        for (int jPixel = 0; jPixel < getTileHeight(); jPixel++) {
          // Offset is in bits
          int offset = (jPixel * getTileWidth()) * bitsPerPixel;
          int endOffset = ((jPixel + 1) * getTileWidth()) * bitsPerPixel;
          // Read first pixel (reference pixel)
          for (int iSample = 0; iSample < numSamples; iSample++) {
            previousPixel[iSample] = (int) MathUtil.getBits(tileData, offset, bitsPerSample[iSample]);
            offset += bitsPerSample[iSample];
          }
          while (offset < endOffset) {
            for (int iSample = 0; iSample < numSamples; iSample++) {
              int diffValue = (int) MathUtil.getBits(tileData, offset, bitsPerSample[iSample]);
              int correctValue = previousPixel[iSample] + diffValue;
              MathUtil.setBits(tileData, offset, bitsPerSample[iSample], correctValue);
              previousPixel[iSample] = correctValue;
              offset += bitsPerSample[iSample];
            }
          }
        }
      }
      predictor = 0;
    } else if (predictor == 3) {
      // Floating-point differencing
      byte[] decompressedTileData = new byte[tileData.length];
      int numSamples = getNumSamples();
      byte[] previousPixel = new byte[numSamples];
      // Scan the data row-by-row as data is encoded for each row separately
      for (int jPixel = 0; jPixel < getTileHeight(); jPixel++) {
        // Offset of the first pixel in this row in *bytes*
        int readOffset = (jPixel * getTileWidth()) * numSamples * 4;
        int endOffset = ((jPixel + 1) * getTileWidth()) * numSamples * 4;
        int writeOffset = (jPixel * getTileWidth()) * numSamples * 4;
        // Read first pixel (reference pixel)
        for (int iSample = 0; iSample < numSamples; iSample++) {
          previousPixel[iSample] = tileData[readOffset];
          decompressedTileData[writeOffset] = previousPixel[iSample];
          readOffset++;
          writeOffset += 4; // Move to next sample
        }
        while (readOffset < endOffset) {
          if (readOffset % (getTileWidth() * numSamples) == 0) {
            // Reached the end of the first component of the floating point number
            writeOffset -= getTileWidth() * numSamples * 4 - 1;
          }
          for (int iSample = 0; iSample < numSamples; iSample++) {
            previousPixel[iSample] += tileData[readOffset];
            decompressedTileData[writeOffset] = previousPixel[iSample];
            readOffset++;
            writeOffset += 4; // Move to next sample
          }
        }
        assert writeOffset == decompressedTileData.length + 3 :
            String.format("Ended at offset %d instead of %d", writeOffset, decompressedTileData.length);
      }
      tileData = decompressedTileData;
      predictor = 0;
      // For some reason, when the floating-point diff predictor is used, data is always in big-endian format
      littleEndian = false;
    }

    return tileData;
  }

  protected void compress() {
    if (compressionScheme == TiffConstants.COMPRESSION_NONE) {
      this.compressionScheme = TiffConstants.COMPRESSION_LZW;
      this.tileData = LZWCodec.encode(tileData);
      if (this.getNumSamples() != 1 && planarConfiguration == TiffConstants.PlanarFormat)
        throw new RuntimeException("Floating-point differencing does not yet support PlanarFormat");


    }
  }

  /**Start of image marker. It is always preceded by 0xff*/
  private static final byte SOI = (byte) 0xd8;

  /**End of image marker. It is always preceded by 0xff*/
  private static final byte EOI = (byte) 0xd9;

  public void decodeJPEG() {
    Iterator<ImageReader> iter = ImageIO.getImageReadersByFormatName("jpeg");

    if(!iter.hasNext())
      throw new RuntimeException("Could not find a JPEG reader");

    ImageReader jpegReader = iter.next();

    byte[] jpegData;
    if (jpegTable != null) {
      // Concatenate the JPEG Table + compressed data into one array for the entire image
      // For proper concatenation, remove the trailing EOI marker from the able and leading SOI marker from the image
      int eoiPosition = jpegTable.length - 2;
      while (eoiPosition >= 0 && !(jpegTable[eoiPosition] == -1 && jpegTable[eoiPosition+1] == EOI))
        eoiPosition--;
      if (eoiPosition < 0)
        eoiPosition = jpegTable.length;
      jpegData = new byte[eoiPosition + tileData.length];
      System.arraycopy(jpegTable, 0, jpegData, 0, eoiPosition);
      int soiPosition = 0;
      if (tileData[0] == -1 && tileData[1] == SOI)
        soiPosition = 2;
      System.arraycopy(tileData, soiPosition, jpegData, eoiPosition, tileData.length - soiPosition);
    } else {
      jpegData = tileData;
    }
    ImageInputStream iis = new MemoryCacheImageInputStream(new ByteArrayInputStream(jpegData));
    jpegReader.setInput(iis);
    ImageReadParam param = jpegReader.getDefaultReadParam();
    BufferedImage targetImage;
    int imageType;
    switch (getNumSamples()) {
      case 1: imageType = BufferedImage.TYPE_BYTE_GRAY; break;
      case 3: imageType = BufferedImage.TYPE_INT_RGB; break;
      case 4: imageType = BufferedImage.TYPE_INT_ARGB; break;
      default: throw new RuntimeException("Unsupported number of bands for JPEG "+getNumSamples());
    }
    targetImage = new BufferedImage(getTileWidth(), getTileHeight(), imageType);
    param.setDestination(targetImage);
    try {
      jpegReader.read(0, param);
      // Read back into a byte array
      assert bitsPerPixel == 8 * getNumSamples() : "JPEG requires bits per sample to be 8";
      tileData = new byte[getTileWidth() * getTileHeight() * getNumSamples()];
      int offset = 0;
      int[] pixelValue = new int[getNumSamples()];
      for (int y = 0; y < getTileHeight(); y++)
        for (int x = 0; x < getTileWidth(); x++) {
          targetImage.getRaster().getPixel(x, y, pixelValue);
          for (int s = 0; s < pixelValue.length; s++)
            tileData[offset++] = (byte) (pixelValue[s] & 0xff);
        }
    } catch (IOException e) {
      throw new RuntimeException("Error reading JPEG");
    }
  }
}
