package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlipNonReferenceMatchExpression extends AbstractNormalizeFilters {
    private static final FlipNonReferenceMatchExpression INSTANCE = new FlipNonReferenceMatchExpression();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, FlipNonReferenceMatchExpression.INSTANCE);
    }

    @Override
    public Condition onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
        // check each child - if we pass all checks we will give up
        boolean rewrite = target.hasLiteral();
        for (Value candidate : candidatesList) {
            if (candidate.hasReference()) {
                rewrite = true;
                break;
            }
        }
        if (!rewrite) {
            return NormalizeFilterUtil.doIn(target, candidatesList, caseSensitivity, matchType);
        }

        if (candidatesList.size() == 1) {
            // make a single node to replace with, just swap the order of the two children
            return NormalizeFilterUtil.doIn(candidatesList.get(0), Collections.singletonList(target), caseSensitivity, matchType);
        }

        // make a AND/OR to join each of the new children with
        List<Condition> replacementChildren = new ArrayList<>();

        for (Value candidate : candidatesList) {
            replacementChildren.add(NormalizeFilterUtil.doIn(candidate, Collections.singletonList(target), caseSensitivity, matchType));
        }

        // wrap each of the new operations in an AND or OR as necessary
        if (matchType == MatchType.REGULAR) {
            return NormalizeFilterUtil.doOr(replacementChildren);
        } else {
            return NormalizeFilterUtil.doAnd(replacementChildren);
        }
    }
}
