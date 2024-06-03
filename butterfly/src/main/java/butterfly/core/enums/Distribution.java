package butterfly.core.enums;

/**
 * @author Lynn Lee
 * @date 2024/1/11
 **/
public enum Distribution {
    UNIFORM,
    DIAGONAL,
    GAUSSIAN,
    BIT,
    SIERPINSKI,
    PARCELL;

   public static Distribution getDistributionType(String str) {
        for (Distribution me : Distribution.values()) {
            if (me.name().equalsIgnoreCase(str)) {
                return me;
            }
        }
        throw new IllegalArgumentException("[" + Distribution.class + "] Unsupported file type:" + str);
    }
}
