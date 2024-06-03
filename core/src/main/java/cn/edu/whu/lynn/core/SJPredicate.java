package cn.edu.whu.lynn.core;

/**
 * Enumerated type for the spatial join predicate
 */
// TODO: Move this to a separate file
// TODO： 研究点：更多空间连接谓词支持和实验 (如：Contains, Within, Touches, Crosses, Overlaps, Equals, Covers, CoveredBy, Intersects)
public enum SJPredicate {
    Intersects,
    MBRIntersects,
    Contains,

    // TODO: 以下谓词直接调用JTS，需要测试
    Within,
    Covers,
    Covered_by,
    Touches,
    Overlaps,
    Crosses,
    Equals,
    Disjoint;

    public static SJPredicate getSJPredicate(String str) {
        if (str == null) return null;
        for (SJPredicate me : SJPredicate.values()) {
            if (me.name().equalsIgnoreCase(str)) {
                return me;
            }
        }
        throw new IllegalArgumentException("[" + SJPredicate.class + "] Unsupported type:" + str);
    }
}
