package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see TableOperations#meta()
 */
@Immutable
@NodeStyle
public abstract class MetaTable extends TableBase implements SingleParentTable {

    public static TableSpec of(TableSpec parent) {
        return ImmutableMetaTable.of(parent);
    }

    @Parameter
    public abstract TableSpec parent();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
