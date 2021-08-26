package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.Condition;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Any AND nested within another AND or OR nested within another OR should be flattened into just a single level.
 *
 * This should be run after NOTs are distributed (so that (A AND B AND !(C OR D)) is first normalized to (A AND B AND
 * (!C AND !D))).
 */
public class MergeNestedBinaryOperations extends AbstractNormalizeFilters {
    private static final MergeNestedBinaryOperations INSTANCE = new MergeNestedBinaryOperations();

    public static Condition exec(Condition filter) {
        return FilterVisitor.accept(filter, MergeNestedBinaryOperations.INSTANCE);
    }

    @Override
    public Condition onAnd(List<Condition> filtersList) {
        List<Condition> visited = new ArrayList<>();
        // walk the descendents, recursing into AND nodes, but only copying non-AND nodes into our result list
        collect(filtersList, visited, Condition::hasAnd, c -> c.getAnd().getFiltersList());

        // before actually wrapping, visit children in case there are ANDs or ORs further nested
        return NormalizeFilterUtil.doAnd(visited, this);
    }

    private void collect(List<Condition> filtersList, List<Condition> visited, Predicate<Condition> matches,
            Function<Condition, List<Condition>> getChildren) {
        for (Condition condition : filtersList) {
            if (matches.test(condition)) {
                // recurse, find more
                collect(getChildren.apply(condition), visited, matches, getChildren);
            } else {
                visited.add(condition);
            }
        }
    }

    @Override
    public Condition onOr(List<Condition> filtersList) {
        List<Condition> visited = new ArrayList<>();
        // walk the descendents, recursing into OR nodes, but only copying non-OR nodes into our result list
        collect(filtersList, visited, Condition::hasOr, c -> c.getOr().getFiltersList());

        // before actually wrapping, visit children in case there are ANDs or ORs further nested
        return NormalizeFilterUtil.doOr(visited, this);
    }
}
