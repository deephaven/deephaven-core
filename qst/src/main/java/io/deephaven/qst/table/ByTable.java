package io.deephaven.qst.table;

import io.deephaven.api.Selectable;
import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see io.deephaven.api.TableOperations#by(Collection)
 */
@Immutable
@NodeStyle
public abstract class ByTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableByTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<Selectable> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {
        Builder parent(TableSpec parent);

        Builder addColumns(Selectable element);

        Builder addColumns(Selectable... elements);

        Builder addAllColumns(Iterable<? extends Selectable> elements);

        ByTable build();
    }
}
