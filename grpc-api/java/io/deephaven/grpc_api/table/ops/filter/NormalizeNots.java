package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rewrites NOT expressions, with the goal of removing as many as possible and simplifying the expression tree.
 * <ul>
 *     <li>Distribute NOTs to children of AND/OR expressions, via DeMorgan's law.</li>
 *     <li>{@code NOT(NOT(A))} is replaced with A.</li>
 *     <li>Replace any operation with its opposite, if any. For example, {@code NOT(A >= B)} is replaced with
 *     {@code A < B}, and likewise for all the other inequality operators, {@code EQ}, and {@code IN}.
 *     </li>
 *     <li>Other operations {@code IS_NULL}, {@code INVOKE}, {@code SEARCH}, {@code CONTAINS} are left as-is, wrapped
 *     wrapped with a {@code NOT}.</li>
 * </ul>
 */
public class NormalizeNots extends ReplacingFilterVisitor.AbstractReplacingFilterVisitor {

    public static Condition exec(Condition filter) {
        return ReplacingFilterVisitor.accept(filter, new NormalizeNots());
    }

    @Override
    public Condition onNot(Condition filter) {
        return ReplacingFilterVisitor.accept(filter, new DistributeNotVisitor());
    }

    class DistributeNotVisitor extends AbstractReplacingFilterVisitor {
        @Override
        public Condition onAnd(List<Condition> filtersList) {
            return Condition.newBuilder()
                    .setOr(OrCondition.newBuilder()
                            .addAllFilters(filtersList.stream().map(c -> ReplacingFilterVisitor.accept(c, NormalizeNots.this)).collect(Collectors.toList()))
                            .build())
                    .build();
        }
        @Override
        public Condition onOr(List<Condition> filtersList) {
            return Condition.newBuilder()
                    .setAnd(AndCondition.newBuilder()
                            .addAllFilters(filtersList.stream().map(c -> ReplacingFilterVisitor.accept(c, NormalizeNots.this)).collect(Collectors.toList()))
                            .build())
                    .build();
        }
        @Override
        public Condition onNot(Condition filter) {
            // unwrap, then visit by parent looking for more NOTs
            return ReplacingFilterVisitor.accept(filter, NormalizeNots.this);
        }

        @Override
        public Condition onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
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
            return super.onComparison(operation, lhs, rhs);
        }

        @Override
        public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
            return super.onIn(target, candidatesList, caseSensitivity, invertMatchType(matchType));
        }

        @NotNull
        public MatchType invertMatchType(MatchType matchType) {
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
        public Condition onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {
            return super.onContains(reference, searchString, caseSensitivity, invertMatchType(matchType));
        }

        @Override
        public Condition onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
            return super.onMatches(reference, regex, caseSensitivity, invertMatchType(matchType));
        }

        // these operations don't have a corresponding NOT, we have to wrap them as before
        @Override
        public Condition onIsNull(Reference reference) {
            return Condition.newBuilder().setNot(NotCondition.newBuilder()
                    .setFilter(Condition.newBuilder().setIsNull(IsNullCondition.newBuilder()
                            .setReference(reference)
                            .build()).build())
                    .build()).build();
        }

        @Override
        public Condition onInvoke(String method, Value target, List<Value> argumentsList) {
            return Condition.newBuilder().setNot(NotCondition.newBuilder()
                    .setFilter(Condition.newBuilder().setInvoke(InvokeCondition.newBuilder()
                            .setMethod(method)
                            .setTarget(target)
                            .addAllArguments(argumentsList)
                            .build()).build())
                    .build()).build();
        }

        @Override
        public Condition onSearch(String searchString, List<Reference> optionalReferencesList) {
            throw new IllegalStateException("Cannot not() a search");
        }
    }
}
