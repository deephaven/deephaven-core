//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.function.Function;

final class LogicalUnionAdapter {

    public static TableSpec of(LogicalUnion union, RelNodeAdapter adapter) {
        final MergeTable.Builder builder = MergeTable.builder();
        for (RelNode input : union.getInputs()) {
            builder.addTables(adapter.table(input));
        }
        final MergeTable mergeTable = builder.build();
        if (union.all) {
            return mergeTable;
        } else {
            return mergeTable.selectDistinct();
        }
    }
}
