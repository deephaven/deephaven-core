package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Normalizes expressions, with the goal of removing as many as possible and simplifying the
 * expression tree. This classes passes through all operations to NormalizeUtil *except* the Not
 * operation, which it rewrites by using the NormalizeInvertedFilters visitor defined later in this
 * file.
 */
public class NormalizeNots extends AbstractNormalizeFilters {
    private static final NormalizeNots INSTANCE = new NormalizeNots();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, NormalizeNots.INSTANCE);
    }

    private NormalizeNots() {}

    @Override
    public Condition onNot(Condition filter) {
        return FilterVisitor.accept(filter, NormalizeInvertedFilters.INSTANCE);
    }

    /**
     * Normalizes *and inverts* expressions, with the goal of removing as many as possible and
     * simplifying the expression tree. This class rewrites and, or, comparisons, and matches, and
     * nots:
     * <ul>
     * <li>Distribute NOTs to children of AND/OR expressions, via DeMorgan's law.</li>
     * <li>{@code NOT(NOT(A))} is replaced with A.</li>
     * <li>Replace any operation with its opposite, if any. For example, {@code NOT(A >= B)} is
     * replaced with {@code A < B}, and likewise for all the other inequality operators, {@code EQ},
     * and {@code IN}.</li>
     * <li>Other operations {@code IS_NULL}, {@code INVOKE}, {@code SEARCH}, {@code CONTAINS} are
     * left as-is, wrapped wrapped with a {@code NOT}.</li>
     * </ul>
     */
    static class NormalizeInvertedFilters implements FilterVisitor<Condition> {
        private static final NormalizeInvertedFilters INSTANCE = new NormalizeInvertedFilters();

        private NormalizeInvertedFilters() {}

        @Override
        public Condition onAnd(List<Condition> filtersList) {
            // not(and(a,b)) == or(not a, not b)
            return NormalizeFilterUtil.doOr(filtersList, this);
        }

        @Override
        public Condition onOr(List<Condition> filtersList) {
            // not(or(a,b)) == and(not a, not b)
            return NormalizeFilterUtil.doAnd(filtersList, this);
        }

        @Override
        public Condition onNot(Condition filter) {
            // not(not(a)) == a
            // So we pass down the regular normalizing filter, not this inverted one.
            return FilterVisitor.accept(filter, NormalizeNots.INSTANCE);
        }

        @Override
        public Condition onComparison(CompareCondition.CompareOperation operation,
            CaseSensitivity caseSensitivity, Value lhs, Value rhs) {
            // Invert the condition
            switch (operation) {
                case LESS_THAN:
                    operation = CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL;
                    break;
                case LESS_THAN_OR_EQUAL:
                    operation = CompareCondition.CompareOperation.GREATER_THAN;
                    break;
                case GREATER_THAN:
                    operation = CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    operation = CompareCondition.CompareOperation.LESS_THAN;
                    break;
                case EQUALS:
                    operation = CompareCondition.CompareOperation.NOT_EQUALS;
                    break;
                case NOT_EQUALS:
                    operation = CompareCondition.CompareOperation.EQUALS;
                    break;
                case UNRECOGNIZED:
                default:
                    throw new UnsupportedOperationException("Unknown operation " + operation);
            }
            return NormalizeFilterUtil.doComparison(operation, caseSensitivity, lhs, rhs);
        }

        @Override
        public Condition onIn(Value target, List<Value> candidatesList,
            CaseSensitivity caseSensitivity, MatchType matchType) {
            return NormalizeFilterUtil.doIn(target, candidatesList, caseSensitivity,
                invertMatchType(matchType));
        }

        @NotNull
        private static MatchType invertMatchType(MatchType matchType) {
            switch (matchType) {
                case REGULAR:
                    matchType = MatchType.INVERTED;
                    break;
                case INVERTED:
                    matchType = MatchType.REGULAR;
                    break;
                case UNRECOGNIZED:
                default:
                    throw new UnsupportedOperationException("Unknown matchType " + matchType);
            }
            return matchType;
        }

        @Override
        public Condition onContains(Reference reference, String searchString,
            CaseSensitivity caseSensitivity, MatchType matchType) {
            return NormalizeFilterUtil.doContains(reference, searchString, caseSensitivity,
                invertMatchType(matchType));
        }

        @Override
        public Condition onMatches(Reference reference, String regex,
            CaseSensitivity caseSensitivity, MatchType matchType) {
            return NormalizeFilterUtil.doMatches(reference, regex, caseSensitivity,
                invertMatchType(matchType));
        }

        @Override
        public Condition onIsNull(Reference reference) {
            // This operation doesn't have a corresponding NOT, so we have to wrap it as before.
            return NormalizeFilterUtil.doInvert(NormalizeFilterUtil.doIsNull(reference));
        }

        @Override
        public Condition onInvoke(String method, Value target, List<Value> argumentsList) {
            // This operation doesn't have a corresponding NOT, we have to wrap them as before
            return NormalizeFilterUtil
                .doInvert(NormalizeFilterUtil.doInvoke(method, target, argumentsList));
        }

        @Override
        public Condition onSearch(String searchString, List<Reference> optionalReferencesList) {
            throw new IllegalStateException("Cannot not() a search");
        }
    }
}

