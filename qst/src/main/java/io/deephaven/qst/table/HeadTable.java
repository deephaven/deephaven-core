//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see io.deephaven.api.TableOperations#head(long)
 */
@Immutable
@NodeStyle
public abstract class HeadTable extends TableBase implements SingleParentTable {

    public static HeadTable of(TableSpec parent, long size) {
        return ImmutableHeadTable.of(parent, size);
    }

    @Parameter
    public abstract TableSpec parent();

    @Parameter
    public abstract long size();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException(
                    String.format("head must have a non-negative size: %d", size()));
        }
    }
}
