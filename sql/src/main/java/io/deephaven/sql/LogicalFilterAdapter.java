//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.filter.Filter;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.WhereTable;
import org.apache.calcite.rel.logical.LogicalFilter;

final class LogicalFilterAdapter {

    public static TableSpec indexTable(LogicalFilter filter, IndexRef indexRef) {
        return of(filter, indexRef, indexRef);
    }

    private static TableSpec of(LogicalFilter filter, RelNodeAdapter nodeAdapter, FieldAdapter fieldAdapter) {
        final TableSpec parent = nodeAdapter.table(filter.getInput());
        final Filter condition = fieldAdapter.filter(filter, filter.getCondition());
        return WhereTable.of(parent, condition);
    }
}
