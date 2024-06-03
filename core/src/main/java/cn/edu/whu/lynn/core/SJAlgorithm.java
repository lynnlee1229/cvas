package cn.edu.whu.lynn.core;

/**
 * Enumerated type for the distributed spatial join algorithms
 */
public enum SJAlgorithm {
    BNLJ,
    PBSM,
    SJMR,
    DJ,
    REPJ,
    SJ1D,
    INLJ,
    BJ// broadcast join
    ;

    public static SJAlgorithm getSJAlgorithm(String str) {
        if (str == null) return null;
        for (SJAlgorithm me : SJAlgorithm.values()) {
            if (me.name().equalsIgnoreCase(str)) {
                return me;
            }
        }
        throw new IllegalArgumentException("[" + SJAlgorithm.class + "] Unsupported type:" + str);
    }
}
