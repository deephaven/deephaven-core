/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterComparison.Operator;
import io.deephaven.qst.table.JoinTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.WhereTable;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.ArrayList;
import java.util.List;

final class LogicalJoinAdapter {
    public static TableSpec indexTable(SqlRootContext rootContext, LogicalJoin join, IndexRef indexRef) {
        // C_0, C_1, ... C_{ L - 1 }
        final TableSpec left = RelNodeAdapterIndexRef.of(rootContext, join.getLeft(), indexRef);
        // C_L, ..., C_{ L + R - 1 }
        final TableSpec right = RelNodeAdapterIndexRef.of(rootContext, join.getRight(),
                indexRef.shifted(join.getLeft().getRowType().getFieldCount()));

        final JoinTable.Builder builder = JoinTable.builder()
                .left(left)
                .right(right);

        // general filter parsing
        // we _really_ need join-aware contextual parsing here to handle SQLTODO(input-ref-match-hack) concerns
        final Filter condition = indexRef.filter(join, join.getCondition());

        final List<Filter> postFilterConditions = new ArrayList<>();

        // SQLTODO(pre-join-condition-expressions)
        //
        // There may be cases where the join condition is a more complex expression.
        // For example, LHS.id = RHS.id + 1.
        //
        // Ideally, calcite optimizations would do expression pushdown for us (and it _might_ if configured correctly),
        // and hopefully filter pushdown where applicable.

        for (Filter filter : Filter.extractAnds(FilterSimplifier.of(condition))) {
            if (!(filter instanceof FilterComparison)) {
                postFilterConditions.add(filter);
                continue;
            }
            final FilterComparison fc = (FilterComparison) filter;
            if (fc.operator() != Operator.EQUALS) {
                postFilterConditions.add(filter);
                continue;
            }
            if (!(fc.lhs() instanceof ColumnName) || !(fc.rhs() instanceof ColumnName)) {
                postFilterConditions.add(filter);
                continue;
            }
            // SQLTODO(input-ref-match-hack)
            //
            // By default, calcite does not re-arrange conditions to be more "traditional". For example,
            // RHS.id = LHS.id does not seem to be re-arranged w/ LHS first. As part of our expression parsing, DH does
            // some of this re-arranging.
            //
            // When we get to this stage, we are making a (potentially bad) assumption that fc.lhs() _is_ from the left
            // table and fc.rhs() _is_ from the right table. For example, LHS.id = LHS.id2 would break this assumption.
            builder.addMatches(JoinMatch.of((ColumnName) fc.lhs(), (ColumnName) fc.rhs()));
        }
        final JoinTable joinTable = builder.build();
        if (postFilterConditions.isEmpty()) {
            return joinTable;
        }
        return WhereTable.of(joinTable, Filter.and(postFilterConditions));
    }
}
