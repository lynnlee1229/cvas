package cn.edu.whu.lynn.io

import cn.edu.whu.lynn.indexing.{RTreeFeatureReader, RTreeFeatureWriter}

class RTreeSource extends SpatialFileSourceSink[RTreeFeatureReader, RTreeFeatureWriter] {

  /**A short name to use by users to access this source */
  override def shortName(): String = "rtree"

}
