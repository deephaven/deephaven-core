package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.List;

/**
 * Rewrite any IN-type expression into its corresponding EQUALS if the left side is not a reference
 * or if the right side does have a reference. Assumes that FlipNonReferenceMatchExpression has
 * already been run, making this the second attempt to deal with these, and letting us be confident
 * that these expressions cannot be expressed as more efficient "in"s.
 *
 * Examples: o ColumnA in 1 - left as is o ColumnA in 1, 2 - left as is o 1 in 2 - rewritten to 1 ==
 * 2. o ColumnA in ColumnB - rewritten to ColumnA == ColumnB
 *
 * Signs that visitors were mis-ordered: o 1 in ColumnA - literal on LHS should already be handled o
 * 1 in 2, 3 - literal on LHS with multiple RHS values should already be handled, should have been
 * flipped and split into individual exprs o ColumnA in ColumnB, 2 - column ref on RHS should
 * already be handled
 */
public class ConvertInvalidInExpressions extends AbstractNormalizeFilters {
    private static final ConvertInvalidInExpressions INSTANCE = new ConvertInvalidInExpressions();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, ConvertInvalidInExpressions.INSTANCE);
    }

    @Override
    public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity,
        MatchType matchType) {
        if (candidatesList.size() != 1
            || target.getDataCase() != candidatesList.get(0).getDataCase()) {
            return super.onIn(target, candidatesList, caseSensitivity, matchType);
        }

        return NormalizeFilterUtil.doComparison(operation(matchType), caseSensitivity, target,
            candidatesList.get(0));
    }

    private CompareCondition.CompareOperation operation(MatchType matchType) {
        switch (matchType) {
            case REGULAR:
                return CompareCondition.CompareOperation.EQUALS;
            case INVERTED:
                return CompareCondition.CompareOperation.NOT_EQUALS;
        }
        throw new IllegalStateException("Can't handle matchType" + matchType);
    }
}
