package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.grpc_api.table.ops.filter.NormalizeFilterUtil.*;

/**
 * Rewrites logical expressions into an actual version that does what would be expected. Right now
 * this is just equalsIgnoreCase and its negation, to support null values.
 *
 * Note that some of the branches here should not be needed (such as
 * reference.equalsIgnoreCase("literal")) as they should be replaced by a MatchFilter instead, but
 * this may not be fully implemented, so we are defensively leaving these cases in place for now.
 */
public class MakeExpressionsNullSafe extends AbstractNormalizeFilters {
    private static final MakeExpressionsNullSafe INSTANCE = new MakeExpressionsNullSafe();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, MakeExpressionsNullSafe.INSTANCE);
    }

    @Override
    public Condition onComparison(CompareCondition.CompareOperation operation,
        CaseSensitivity caseSensitivity, Value lhs, Value rhs) {
        // only apply to ==/!= operations that are case insensitive
        if (caseSensitivity == CaseSensitivity.MATCH_CASE) {
            return super.onComparison(operation, caseSensitivity, lhs, rhs);
        }
        if (caseSensitivity != CaseSensitivity.IGNORE_CASE) {
            throw new IllegalStateException("Unrecognized case sensitivity " + caseSensitivity);
        }

        // if lhs is not a reference, we aren't worried about null, can emit the safe call without a
        // null check
        Condition equalsIgnoreCase =
            doInvoke("equalsIgnoreCase", lhs, Collections.singletonList(rhs));
        if (lhs.hasLiteral()) {
            // the left side of the compare is a literal, so we emit something like
            // "foo".equalsIgnoreCase(right-hand-side)
            return equalsIgnoreCase;
        }

        Reference lhsRef = lhs.getReference();

        Condition lhsNullCheck = doAnd(Arrays.asList(
            doInvert(doIsNull(lhsRef)),
            equalsIgnoreCase));
        if (rhs.hasLiteral()) {
            // if rhs isn't a reference, it cannot be null, and we don't need to worry about the
            // second
            // null check, so we emit something like !isNull(foo) &&
            // foo.equalsIgnoreCase(right-hand-side)
            return lhsNullCheck;
        }

        // both sides could be null, so add a check where either one could be null
        // (foo == null && bar == null) || (foo != null && foo.equalsIgnoreCase(bar))
        return doOr(Arrays.asList(
            doAnd(Arrays.asList(
                doIsNull(lhsRef),
                doIsNull(rhs.getReference()))),
            lhsNullCheck));
    }
}
