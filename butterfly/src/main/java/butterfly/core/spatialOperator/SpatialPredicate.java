

package butterfly.core.spatialOperator;

/**
 * Spatial predicates for range queries and join queries.
 * Please refer to <a href="https://en.wikipedia.org/wiki/DE-9IM#Spatial_predicates">Spatial predicates</a>
 * for the semantics of these spatial predicates.
 */
public enum SpatialPredicate {
    CONTAINS,
    INTERSECTS,
    WITHIN,
    COVERS,
    COVERED_BY,
    TOUCHES,
    OVERLAPS,
    CROSSES,
    EQUALS,
    DISJOINT;

    /**
     * Get inverse predicate of given spatial predicate
     *
     * @param predicate spatial predicate
     * @return inverse predicate
     */
    public static SpatialPredicate inverse(SpatialPredicate predicate) {
        switch (predicate) {
            case CONTAINS:
                return SpatialPredicate.WITHIN;
            case WITHIN:
                return SpatialPredicate.CONTAINS;
            case COVERS:
                return SpatialPredicate.COVERED_BY;
            case COVERED_BY:
                return SpatialPredicate.COVERS;
            default:
                return predicate;
        }
    }
}
