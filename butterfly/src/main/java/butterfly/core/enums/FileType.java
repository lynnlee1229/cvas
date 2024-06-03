package butterfly.core.enums;

import java.io.Serializable;

/**
 * @author Lynn Lee
 * @date 2023/12/29
 **/
public enum FileType implements Serializable{
    CSVPOINT,
    WKT,
    SHAPEFILE,
    GEOJSON;
    public static FileType getFileType(String str) {
        for (FileType me : FileType.values()) {
            if (me.name().equalsIgnoreCase(str)) {
                return me;
            }
        }
        throw new IllegalArgumentException("[" + FileType.class + "] Unsupported file type:" + str);
    }
}
