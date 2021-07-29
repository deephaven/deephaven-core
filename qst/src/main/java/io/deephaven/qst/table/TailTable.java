package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#tail(long)
 */
@Immutable
@NodeStyle
public abstract class TailTable extends TableBase implements SingleParentTable {

    public static TailTable of(TableSpec parent, long size) {
        return ImmutableTailTable.of(parent, size);
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
                String.format("tail must have a non-negative size: %d", size()));
        }
    }
}
