/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#snapshot()
 */
@Immutable
@NodeStyle
public abstract class SnapshotTable extends TableBase implements SingleParentTable {
    public static SnapshotTable of(TableSpec base) {
        return ImmutableSnapshotTable.of(base);
    }

    @Parameter
    public abstract TableSpec base();

    @Override
    public final TableSpec parent() {
        return base();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
