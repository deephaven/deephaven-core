//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.qst.table.AggregateTable;
import io.deephaven.qst.table.SelectDistinctTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.ViewTable;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

final class LogicalAggregateAdapter {

    public static TableSpec namedTable(SqlRootContext rootContext, LogicalAggregate aggregate) {
        return of(rootContext, aggregate, rootContext.namedAdapter());
    }

    public static TableSpec indexTable(SqlRootContext rootContext, LogicalAggregate aggregate, IndexRef indexRef) {
        return of(rootContext, aggregate, indexRef);
    }

    private static TableSpec of(SqlRootContext rootContext, LogicalAggregate aggregate, FieldAdapter fieldAdapter) {
        final IndexRef indexInputRef = rootContext.createIndexRef(Prefix.AGGREGATE, aggregate);
        final TableSpec parent = indexInputRef.table(aggregate.getInput());
        final int groupCount = aggregate.getGroupCount();
        final int outputFieldCount = aggregate.getRowType().getFieldCount();
        final List<ColumnName> groupByColumns = new ArrayList<>(groupCount);
        Iterator<Integer> groupInputIt = aggregate.getGroupSet().iterator();
        int groupI = 0;
        for (; groupI < groupCount && groupInputIt.hasNext(); ++groupI) {
            final int groupInputIndex = groupInputIt.next();
            groupByColumns.add(Helper.inputColumnName(aggregate, groupInputIndex, indexInputRef));
        }
        if (groupI != groupCount || groupInputIt.hasNext()) {
            throw new IllegalStateException();
        }
        final TableSpec aggTable;
        if (groupCount == outputFieldCount) {
            // this is how calcite expresses SELECT DISTINCT
            aggTable = SelectDistinctTable.builder().parent(parent).addAllColumns(groupByColumns).build();
        } else {
            AggregateTable.Builder builder =
                    AggregateTable.builder().parent(parent).addAllGroupByColumns(groupByColumns);
            for (int outFieldIndex = groupCount; outFieldIndex < outputFieldCount; ++outFieldIndex) {
                final AggregateCall aggregateCall = aggregate.getAggCallList().get(outFieldIndex - groupCount);
                final List<ColumnName> inColumns = new ArrayList<>(aggregateCall.getArgList().size());
                for (int inputIndex : aggregateCall.getArgList()) {
                    inColumns.add(Helper.inputColumnName(aggregate, inputIndex, indexInputRef));
                }
                final ColumnName outColumn = Helper.outputColumnName(aggregate, outFieldIndex, fieldAdapter);
                builder.addAggregations(AggregateCallAdapterImpl.aggregation(aggregateCall, outColumn, inColumns));
            }
            aggTable = builder.build();
        }
        if (groupCount == 0) {
            return aggTable;
        }
        // If group columns are present, we need to rename them.
        final ViewTable.Builder viewBuilder = ViewTable.builder().parent(aggTable);
        groupInputIt = aggregate.getGroupSet().iterator();
        for (groupI = 0; groupI < groupCount && groupInputIt.hasNext(); ++groupI) {
            final int groupInputIndex = groupInputIt.next();
            final ColumnName newColumn = Helper.outputColumnName(aggregate, groupI, fieldAdapter);
            final ColumnName existing = Helper.inputColumnName(aggregate, groupInputIndex, indexInputRef);
            viewBuilder.addColumns(Selectable.of(newColumn, existing));
        }
        if (groupI != groupCount || groupInputIt.hasNext()) {
            throw new IllegalStateException();
        }
        for (int i = groupCount; i < outputFieldCount; ++i) {
            viewBuilder.addColumns(Helper.outputColumnName(aggregate, i, fieldAdapter));
        }
        return viewBuilder.build();
    }
}
