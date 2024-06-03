package cn.edu.whu.lynn.core.index.quadSplitedGeom;


import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public interface QuadSplitedGeom extends Serializable {
    public Geometry getOriginal();
}
