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
import cn.edu.whu.lynn.util.MathUtil;

/**
 * A tile from a TIFF file that might not be decompressed. The tile is decompressed lazily when the first pixel value
 * is requested. This allows the tile to be efficiently kept in memory and serialized without decompressing it.
 * This class is designed to
 * be standalone and is not associated with an open TIFF file. This makes it possible to serialize this class over
 * network and read the pixel values on another machine.
 */
@DefaultSerializer(TiffTileSerializer.class)
public abstract class AbstractTiffTile implements ITiffTile {

  /**The index of the first column in the tile (inclusive)*/
  protected final int i1;

  /**The index of the last column in the tile (inclusive)*/
  protected final int i2;

  /**The index of the first row (scanline) in the tile (inclusive)*/
  protected final int j1;

  /**The index of the last row (scanline) in the tile (inclusive)*/
  protected final int j2;

  /**Number of bits for each sample of the pixel, e.g., [8, 8, 8] for a regular RGB image*/
  protected final int[] bitsPerSample;

  /**Number of bits per pixel*/
  protected final int bitsPerPixel;

  /**Planar configuration of the layer*/
  protected final int planarConfiguration;

  /**True if the tile data is stored in little endian format. False for big endian*/
  protected boolean littleEndian;

  /**
   * This field specifies how to interpret each data sample in a pixel. Possible values are:
   * 1 = unsigned integer data
   * 2 = two's complement signed integer data
   * 3 = IEEE floating point data [IEEE]
   * 4 = undefined data format
   */
  protected int[] sampleFormats;

  public AbstractTiffTile(int i1, int j1, int i2, int j2,
                          int[] bitsPerSample, int[] sampleFormats, int bitsPerPixel,
                          int planarConfiguration, boolean littleEndian) {
    this.i1 = i1;
    this.i2 = i2;
    this.j1 = j1;
    this.j2 = j2;
    this.bitsPerSample = bitsPerSample;
    this.bitsPerPixel = bitsPerPixel;
    this.sampleFormats = sampleFormats;
    this.littleEndian = littleEndian;
    this.planarConfiguration = planarConfiguration;
  }

  /**
   * Return the decompressed array of bytes with the tile data. If the tile is not loaded, it lazily loads it.
   * The loaded data is not serialized to keep the serialization cost low.
   * @return the data for the tile as a byte array
   */
  public abstract byte[] getTileData();


  public final int getTileWidth() {
    return i2 - i1 + 1;
  }

  public final int getTileHeight() {
    return j2 - j1 + 1;
  }

  public final int getNumSamples() {
    return bitsPerSample.length;
  }

  /**
   * Return the raw pixel value as integer by concatenating all its sample in one 64-bit long
   * @param iPixel the index of the column of the pixel
   * @param jPixel the index of the row of the pixel
   * @return the raw value of the pixel (non parsed)
   */
  public long getPixel(int iPixel, int jPixel) {
    if (bitsPerPixel > 64)
      throw new RuntimeException(String.format("The pixel value is too big to fit in a 64-bit integer (%d > 64)",
          bitsPerPixel));
    if (iPixel < i1 || iPixel > i2 || jPixel < j1 || jPixel > j2)
      throw new ArrayIndexOutOfBoundsException(String.format("Pixel (%d, %d) is out of range [(%d, %d), (%d, %d)]",
          iPixel, jPixel, i1, j1, i2, j2));
    long val = 0;
    int pixelOffset = (jPixel - j1) * (i2 - i1 + 1) + (iPixel - i1);
    switch (planarConfiguration) {
      case TiffConstants.ChunkyFormat:
        int bitOffset = pixelOffset * bitsPerPixel;
        for (int bps : bitsPerSample) {
          long sampleValue = MathUtil.getBits(getTileData(), bitOffset, bps);
          if (bps == 16)
            sampleValue = TiffRaster.reverseValue(sampleValue, 16);
          val = (val << bps) | sampleValue;
          bitOffset += bps;
        }
        break;
      case TiffConstants.PlanarFormat:
        throw new RuntimeException("Planar format is not yet supported");
    }
    return val;
  }

  /**
   * Returns all the sample values at the given pixel location as integer. If these samples are stored in other formats,
   * e.g., byte or short, they are converted to integer with the same value. If the samples are represented as
   * floating-point numbers, they are rounded to the nearest integer.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param value the array of values to output to
   */
  public void getPixelSamplesAsInt(int iPixel, int jPixel, int[] value) {
    assert value.length == bitsPerSample.length;
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsInt(iPixel, jPixel, iSample);
  }

  public void getPixelSamplesAsFloat(int iPixel, int jPixel, float[] value) {
    assert value.length == bitsPerSample.length;
    for (int iSample = 0; iSample < bitsPerSample.length; iSample++)
      value[iSample] = getSampleValueAsFloat(iPixel, jPixel, iSample);
  }


  /**
   * Get the sample value bits as an integer without interpreting the sample format.
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample at the given pixel
   */
  public long getRawSampleValue(int iPixel, int jPixel, int iSample) {
    long val = 0;
    int pixelOffset = (jPixel - j1) * getTileWidth() + (iPixel - i1);
    switch (planarConfiguration) {
      case TiffConstants.ChunkyFormat:
        int bitOffset = pixelOffset * bitsPerPixel;
        int i = 0;
        while (i < iSample)
          bitOffset += bitsPerSample[i++];
        val = MathUtil.getBits(getTileData(), bitOffset, bitsPerSample[iSample]);
        if (littleEndian)
          val = TiffRaster.reverseValue(val, bitsPerSample[iSample]);
        break;
      case TiffConstants.PlanarFormat:
        throw new RuntimeException("Planar format is not yet supported");
    }
    return val;
  }


  /**
   * Returns the value of the given pixel and band as an integer
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample which band (or sample) to read for the given pixel
   * @return the sample value as integer
   */
  public int getSampleValueAsInt(int iPixel, int jPixel, int iSample) {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case TiffConstants.SAMPLE_IEEE_FLOAT:
        return Math.round(Float.intBitsToFloat((int) val));
      case TiffConstants.SAMPLE_SIGNED_INT:
        //diff return value
        return (short) val;
      case TiffConstants.SAMPLE_UNSIGNED_INT:
      case TiffConstants.SAMPLE_UNDEFINED:
      default:
        return (int) val;
    }
  }

  /**
   * Returns the value of the given pixel and band as a double-precision floating point value
   * @param iPixel the column of the pixel (x-coordinate)
   * @param jPixel the row of the pixel (y-coordinate)
   * @param iSample the index of the sample to read if the raster contains multiple samples, e.g., red-green-blue
   * @return the value of the given sample as float
   */
  public float getSampleValueAsFloat(int iPixel, int jPixel, int iSample) {
    long val = getRawSampleValue(iPixel, jPixel, iSample);
    switch (sampleFormats[iSample]) {
      case TiffConstants.SAMPLE_IEEE_FLOAT:
        return Float.intBitsToFloat((int) val);
      case TiffConstants.SAMPLE_SIGNED_INT:
        return (short)val;
      case TiffConstants.SAMPLE_UNSIGNED_INT:
      case TiffConstants.SAMPLE_UNDEFINED:
      default:
        return (float) val;
    }
  }

  /**
   * Return the type of the given band
   * 1 = unsigned integer data
   * 2 = two's complement signed integer data
   * 3 = IEEE floating point data [IEEE]
   * 4 = undefined data format
   * @param i the number of the band
   * @return the type of that band
   */
  public int getSampleFormat(int i) {
    return sampleFormats[i];
  }
}
