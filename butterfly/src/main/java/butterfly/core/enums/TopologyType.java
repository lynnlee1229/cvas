package butterfly.core.enums;

/**
 * @author Lynn Lee
 * @date 2024/1/23
 **/
public enum TopologyType {
    INTERSECT, CROSSE, OVERLAP, CONTAIN, WITHIN, EQUAL,
    DISJOINT,
    TOUCH,
    COVER, COVEREDBY,
    WITHIN_DISTANCE, P_CONTAINS, N_CONTAINS, P_WITHIN, N_WITHIN, P_BUFFER, N_BUFFER;

    private double distance;

    public TopologyType distance(double distance) {
        if (this != WITHIN_DISTANCE)
            throw new IllegalArgumentException("Only WITHIN_DISTANCE type can assign distance");

        this.distance = distance;
        return this;
    }

    public double getDistance() {
        if (this != WITHIN_DISTANCE) throw new IllegalArgumentException("Only WITHIN_DISTANCE type can get distance");

        return distance;
    }
}
