package cn.edu.whu.lynn.io

import cn.edu.whu.lynn.common.ButterflyOptions
import cn.edu.whu.lynn.geolite.{EnvelopeND, IFeature}
import cn.edu.whu.lynn.io.{FeatureReader, SpatialReaderMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}

@SpatialReaderMetadata(shortName = "fakescalareader", noSplit = true, filter = "*.xyz")
class FakeScalaReader extends FeatureReader {
  override def initialize(inputSplit: InputSplit, conf: ButterflyOptions): Unit = ???

  override def nextKeyValue(): Boolean = ???

  override def getCurrentValue: IFeature = ???

  override def getProgress: Float = ???

  override def close(): Unit = ???
}