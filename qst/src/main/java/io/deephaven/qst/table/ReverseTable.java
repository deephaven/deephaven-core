//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#reverse()
 */
@Immutable
@NodeStyle
public abstract class ReverseTable extends TableBase implements SingleParentTable {

    public static ReverseTable of(TableSpec parent) {
        return ImmutableReverseTable.of(parent);
    }

    @Parameter
    public abstract TableSpec parent();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
