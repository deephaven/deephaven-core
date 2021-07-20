package io.deephaven.qst.table;

import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class MergeTable extends TableBase {

    public static MergeTable of(Table... tables) {
        return MergeTable.builder().addTables(tables).build();
    }

    public static MergeTable of(Iterable<? extends Table> tables) {
        return MergeTable.builder().addAllTables(tables).build();
    }

    public static Builder builder() {
        return ImmutableMergeTable.builder();
    }

    public abstract List<Table> tables();

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

        Builder addTables(Table element);

        Builder addTables(Table... elements);

        Builder addAllTables(Iterable<? extends Table> elements);

        MergeTable build();
    }
}
