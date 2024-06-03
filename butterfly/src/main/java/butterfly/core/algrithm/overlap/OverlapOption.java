package butterfly.core.algrithm.overlap;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2024/1/15
 **/
public enum OverlapOption implements Serializable {
    /**
     * 交集
     */
    INTERSECTION,
    /**
     * 并集
     */
    UNION,
    /**
     * 差集
     */
    DIFFERENCE,
    /**
     * 对称差集
     */
    SYMMETRIC_DIFFERENCE;

    public static OverlapOption getOverlapOption(String str) {
        for (OverlapOption me : OverlapOption.values()) {
            if (me.name().equalsIgnoreCase(str)) {
                return me;
            }
        }
        return null;
    }

}
