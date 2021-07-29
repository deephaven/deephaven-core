package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see TableOperations#updateView(Collection)
 */
@Immutable
@NodeStyle
public abstract class UpdateViewTable extends TableBase implements SelectableTable {

    public static Builder builder() {
        return ImmutableUpdateViewTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends SelectableTable.Builder<UpdateViewTable, Builder> {

    }
}
