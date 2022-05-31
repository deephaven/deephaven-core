package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Enforces that all IN-type expressions have a reference on the left side and only literals on the right side. Any
 * non-reference on the left or reference on the right will trigger the entire IN-type expression being replaced with an
 * OR or AND, with a sub-IN for each expression on the right side.
 *
 *
 * Examples: o ColumnA in 1, 2, 3 - left as-is o ColumnA in 1, 2, ColumnB - rewritten as (ColumnA in 1 OR ColumnA in 2
 * OR ColumnA in ColumnB) o 1 in 3, 4, 5 - will be rewritten as (3 in 1 OR 4 in 1 OR 5 in 1). This is a silly case, but
 * we're not judging. At this step. o 1 in ColumnA, 4, 5 - will be rewritten as (ColumnA in 1 OR 4 in 1 OR 5 in 1) o 1
 * in ColumnA - will be rewritten as ColumnA in 1 o ColumnA in ColumnB - will be rewritten as ColumnB in ColumnA. Note
 * that like the second example, this isn't productive on its own, but as a pair with a reference on the right, it will
 * be noticed by {@link ConvertInvalidInExpressions}.
 *
 * It is assumed that some time after this step, related "in" expressions will be merged together, and that these
 * one-off expressions will get checked later.
 */
public class FlipNonReferenceMatchExpression extends AbstractNormalizeFilters {
    private static final FlipNonReferenceMatchExpression INSTANCE = new FlipNonReferenceMatchExpression();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, FlipNonReferenceMatchExpression.INSTANCE);
    }

    @Override
    public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity,
            MatchType matchType) {
        // check each child - if we pass all checks we will give up
        boolean rewrite = target.hasLiteral();
        if (!rewrite) {
            for (Value candidate : candidatesList) {
                if (candidate.hasReference()) {
                    rewrite = true;
                    break;
                }
            }
        }
        if (!rewrite) {
            return NormalizeFilterUtil.doIn(target, candidatesList, caseSensitivity, matchType);
        }

        if (candidatesList.size() == 1) {
            // make a single node to replace with, just swap the order of the two children
            return NormalizeFilterUtil.doIn(candidatesList.get(0), Collections.singletonList(target), caseSensitivity,
                    matchType);
        }

        // make a AND/OR to join each of the new children with
        List<Condition> replacementChildren = new ArrayList<>();

        for (Value candidate : candidatesList) {
            replacementChildren.add(
                    NormalizeFilterUtil.doIn(candidate, Collections.singletonList(target), caseSensitivity, matchType));
        }

        // wrap each of the new operations in an AND or OR as necessary
        switch (matchType) {
            case REGULAR:
                return NormalizeFilterUtil.doOr(replacementChildren);
            case INVERTED:
                return NormalizeFilterUtil.doAnd(replacementChildren);
            default:
                throw new IllegalStateException("Unrecognized match type " + matchType);
        }
    }
}
