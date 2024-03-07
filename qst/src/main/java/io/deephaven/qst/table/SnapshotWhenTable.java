//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#snapshotWhen(Object, SnapshotWhenOptions)
 */
@Immutable
@NodeStyle
public abstract class SnapshotWhenTable extends TableBase {
    public static SnapshotWhenTable of(TableSpec base, TableSpec trigger, SnapshotWhenOptions options) {
        return ImmutableSnapshotWhenTable.of(base, trigger, options);
    }

    @Parameter
    public abstract TableSpec base();

    @Parameter
    public abstract TableSpec trigger();

    @Parameter
    public abstract SnapshotWhenOptions options();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
