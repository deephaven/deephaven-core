package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Concatenates multiple tables into a single table.
 *
 * <p>
 * The resultant table will have rows from the same table together, in the order they are specified as inputs.
 *
 * <p>
 * When ticking tables grow, they may run out of the 'pre-allocated' space for newly added rows. When more key- space is
 * needed, tables in higher key-space are shifted to yet higher key-space to make room for new rows. Shifts are handled
 * efficiently, but some downstream operations generate a linear O(n) amount of work per shifted row. When possible, one
 * should favor ordering the constituent tables first by static/non-ticking sources followed by tables that are expected
 * to grow at slower rates, and finally by tables that grow without bound.
 */
@Immutable
@NodeStyle
public abstract class MergeTable extends TableBase {

    public static MergeTable of(TableSpec... tables) {
        return MergeTable.builder().addTables(tables).build();
    }

    public static MergeTable of(Iterable<? extends TableSpec> tables) {
        return MergeTable.builder().addAllTables(tables).build();
    }

    public static Builder builder() {
        return ImmutableMergeTable.builder();
    }

    public abstract List<TableSpec> tables();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Value.Check
    final void checkSize() {
        if (tables().size() < 2) {
            throw new IllegalArgumentException("Must merge at least 2 tables");
        }
    }

    public interface Builder {

        Builder addTables(TableSpec element);

        Builder addTables(TableSpec... elements);

        Builder addAllTables(Iterable<? extends TableSpec> elements);

        MergeTable build();
    }
}
