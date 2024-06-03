package butterfly.core.spatialOperator;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;

import java.io.Serializable;

public class SpatialPredicateEvaluators {
    private SpatialPredicateEvaluators() {
    }

    /**
     * SpatialPredicateEvaluator for evaluating spatial predicates.
     */
    public interface SpatialPredicateEvaluator extends Serializable {
        boolean eval(Geometry left, Geometry right);

        boolean eval(PreparedGeometry left, Geometry right);
    }

    public interface ContainsEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.contains(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.contains(right);
        }
    }

    public interface IntersectsEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.intersects(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.intersects(right);
        }
    }

    public interface WithinEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.within(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.within(right);
        }

    }

    public interface CoversEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.covers(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.covers(right);
        }
    }

    public interface CoveredByEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.coveredBy(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.coveredBy(right);
        }
    }

    public interface TouchesEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.touches(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.touches(right);
        }
    }

    public interface OverlapsEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.overlaps(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.overlaps(right);
        }
    }

    public interface CrossesEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.crosses(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.crosses(right);
        }
    }

    public interface EqualsEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.symDifference(right).isEmpty();
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.getGeometry().symDifference(right).isEmpty();
        }
    }

    public interface DisjointEvaluator extends SpatialPredicateEvaluator {
        default boolean eval(Geometry left, Geometry right) {
            return left.disjoint(right);
        }

        default boolean eval(PreparedGeometry left, Geometry right) {
            return left.disjoint(right);
        }
    }

    private static class ConcreteContainsEvaluator implements ContainsEvaluator {
    }

    private static class ConcreteIntersectsEvaluator implements IntersectsEvaluator {
    }

    private static class ConcreteWithinEvaluator implements WithinEvaluator {
    }

    private static class ConcreteCoversEvaluator implements CoversEvaluator {
    }

    private static class ConcreteCoveredByEvaluator implements CoveredByEvaluator {
    }

    private static class ConcreteTouchesEvaluator implements TouchesEvaluator {
    }

    private static class ConcreteOverlapsEvaluator implements OverlapsEvaluator {
    }

    private static class ConcreteCrossesEvaluator implements CrossesEvaluator {
    }

    private static class ConcreteEqualsEvaluator implements EqualsEvaluator {
    }

    private static class ConcreteDisjointEvaluator implements DisjointEvaluator {
    }

    public static SpatialPredicateEvaluator create(SpatialPredicate predicate) {
        switch (predicate) {
            case CONTAINS:
                return new ConcreteContainsEvaluator();
            case INTERSECTS:
                return new ConcreteIntersectsEvaluator();
            case WITHIN:
                return new ConcreteWithinEvaluator();
            case COVERS:
                return new ConcreteCoversEvaluator();
            case COVERED_BY:
                return new ConcreteCoveredByEvaluator();
            case TOUCHES:
                return new ConcreteTouchesEvaluator();
            case OVERLAPS:
                return new ConcreteOverlapsEvaluator();
            case CROSSES:
                return new ConcreteCrossesEvaluator();
            case EQUALS:
                return new ConcreteEqualsEvaluator();
            case DISJOINT:
                return new ConcreteDisjointEvaluator();
            default:
                throw new IllegalArgumentException("Invalid spatial predicate: " + predicate);
        }
    }
}
