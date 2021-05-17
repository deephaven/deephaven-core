package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.*;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.grpc_api.table.ops.filter.NormalizeFilterUtil.*;

/**
 * Rewrites logical expressions into an actual version that does what would be expected. Right now this is just
 * equalsIgnoreCase and its negation, to support null values.
 */
public class MakeExpressionsNullSafe extends AbstractNormalizeFilters {
    private static final MakeExpressionsNullSafe INSTANCE = new MakeExpressionsNullSafe();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, MakeExpressionsNullSafe.INSTANCE);
    }

    @Override
    public Condition onComparison(CompareCondition.CompareOperation operation, CaseSensitivity caseSensitivity, Value lhs, Value rhs) {
        // only apply to ==/!= operations that are case insensitive
        if (caseSensitivity == CaseSensitivity.MATCH_CASE) {
            return super.onComparison(operation, caseSensitivity, lhs, rhs);
        }

        // if lhs is not a reference, we aren't worried about null, can emit the safe call without a null check
        Condition equalsIgnoreCase = doInvoke("equalsIgnoreCase", lhs, Collections.singletonList(rhs));
        if (lhs.hasLiteral()) {
            return equalsIgnoreCase;
        }

        Reference lhsRef = lhs.getReference();

        Condition lhsNullCheck = doAnd(Arrays.asList(
                doInvert(doIsNull(lhsRef)),
                equalsIgnoreCase
        ));
        if (rhs.hasLiteral()) {
            // if rhs isn't a refrence, we don't need to worry about the first null check
            return lhsNullCheck;
        }

        return doOr(Arrays.asList(
                doAnd(Arrays.asList(
                        doIsNull(lhsRef),
                        doIsNull(rhs.getReference())
                )),
                lhsNullCheck
        ));
    }
}
