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
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException(
                String.format("head must have a non-negative size: %d", size()));
        }
    }
}
