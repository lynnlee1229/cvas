package butterfly.core.common;

import cn.edu.whu.lynn.geolite.EnvelopeNDLite;
import cn.edu.whu.lynn.davinci.CommonVisualizationHelper;

/**
 * @author Lynn Lee
 * @date 2023/12/6
 **/
public class ButterflyConstant {
    // default setting
    public static final EnvelopeNDLite worldMercatorMBR = new EnvelopeNDLite(CommonVisualizationHelper.MercatorMapBoundariesEnvelope);
    public static final String PLOT_BY_PARTITION = "plot_by_partition";
    public static final String PLOT_PARTITION_MBR = "plot_partition_mbr";
    public static final Integer DEFAULT_PARTITION_NUM = 16;

    public static final Character DEFAULT_DELIMITER = ',';

    public static final String SUMMARY_SIZE = "synopsissize";
    public static final String BALANCED_PARTITIONING = "balanced";
    public static final String DISJOINT_INDEX = "disjoint";

    // partitioner
    public static final String CELLS = "cells"; // Partitions the space based on existing set of cells, e.g., another file
    public static final String GRID = "grid"; // Partitions the space using a uniform grid with roughly square cells
    public static final String H_CURVE = "hcurve"; // Sorts the sample points based on a Hilbert-curve and partitions the Hilbert-space into ranges with roughly equal number of points
    public static final String KD_TREE = "kdtree"; // Recursively partitions the space in half (based on the median) alternating between the dimensions until a specific number of partitions is reached.
    public static final String R_GROVE = "rgrove"; // Recursively applies the R-tree node splitting algorithm to split a sample of points into partitions.
    public static final String RRSGROVE = "rrsgrove"; // A partitioner that uses the RR*-tree node splitting algorithm on a sample of points to partition the space
    public static final String RSGROVE = "rsgrove"; // A partitioner that uses the R*-tree node splitting algorithm on a sample of points to partition the space
    public static final String STR = "str"; // Partitions the space using the Sort-tile-recursive (STR) algorithm for R-tree packing
    public static final String Z_CURVE = "zcurve"; // Sorts the sample points based on a Z-curve and partitions the Z-space into ranges with roughly equal number of points


    // default path
    public static final String TEST_OUT_PATH = "/Users/lynnlee/Data/butterfly/testout/";
    public static final String TEST_IN_PATH = "/Users/lynnlee/Data/butterfly/testin/";

    // shenzhen
    public static final String SHENZHEN_BOUNDRY = TEST_IN_PATH + "test-shp-shenzhen/shenzhen_bnd.shp";
    public static final String SHENZHEN_LANDUSE_SHP = TEST_IN_PATH + "test-shp-shenzhen/shenzhen-landuse.shp";
    public static final String SHENZHEN_LANDUSE_CSV = TEST_IN_PATH + "shenzhen_landuse.csv";
    // beijing & tdrive
    public static final String BEIJING_BOUNDRY = TEST_IN_PATH + "test-shp-beijing/beijing_bnd.shp";
    public static final String BEIJING_DISTRICT = TEST_IN_PATH + "test-shp-beijing/beijing_district.shp";
    public static final String BEIJING_LANDUSE = TEST_IN_PATH + "test-shp-beijing/beijing_landuse.shp";
    public static final String TDRIVE = TEST_IN_PATH + "tdrive_merge.txt";
    // datagen
    public static final String UNIFORM_POINTS_10K = TEST_IN_PATH + "uniformPoint10k.wkt";
    public static final String UNIFORM_POLYGONS_10K = TEST_IN_PATH + "uniformPolygon10k.wkt";
    // landuse max
    public static final String LAND_USE_MAX = TEST_IN_PATH + "landuse_metric_with_geom_max.shp";
    public static final String WUHAN_LANDUSE_SHP = TEST_IN_PATH + "test-shp-wuhan/wuhan_landuse.shp";
}