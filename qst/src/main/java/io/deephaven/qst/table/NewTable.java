package io.deephaven.qst.table;

import io.deephaven.qst.LeafStyle;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

@Immutable
@LeafStyle
public abstract class NewTable extends TableBase implements Iterable<Column<?>> {
    public interface Builder {

        Builder size(int size);

        Builder putColumns(String name, Array<?> value);

        default Builder addColumns(Column<?> column) {
            putColumns(column.name(), column.array());
            return this;
        }

        default Builder addColumns(Column<?>... columns) {
            for (Column<?> column : columns) {
                addColumns(column);
            }
            return this;
        }

        default Builder addAllColumns(Iterable<Column<?>> columns) {
            for (Column<?> column : columns) {
                addColumns(column);
            }
            return this;
        }

        NewTable build();
    }

    public static Builder builder() {
        return ImmutableNewTable.builder();
    }

    public static NewTable empty(TableHeader header) {
        Builder builder = builder().size(0);
        for (ColumnHeader<?> columnHeader : header) {
            builder.putColumns(columnHeader.name(), Array.empty(columnHeader.type()));
        }
        return builder.build();
    }

    public static NewTable of(Column<?>... columns) {
        final int size = columns.length > 0 ? columns[0].size() : 0;
        return builder().size(size).addColumns(columns).build();
    }

    public static NewTable of(Iterable<Column<?>> columns) {
        Iterator<Column<?>> it = columns.iterator();
        final int size = it.hasNext() ? it.next().size() : 0;
        return ImmutableNewTable.builder().addAllColumns(columns).size(size).build();
    }

    abstract Map<String, Array<?>> columns();

    // note: size is necessary to handle an empty table

    public abstract int size();

    public final int numColumns() {
        return columns().size();
    }

    public final Array<?> getArray(String name) {
        return columns().get(name);
    }

    public final TableHeader header() {
        TableHeader.Builder builder = TableHeader.builder();
        for (Entry<String, Array<?>> e : columns().entrySet()) {
            builder.putHeaders(e.getKey(), e.getValue().type());
        }
        return builder.build();
    }

    public final NewTable with(Column<?> column) {
        return builder().size(size()).addAllColumns(this).addColumns(column).build();
    }

    @Override
    public final <V extends Table.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkColumnsSizes() {
        if (!columns().values().stream().mapToInt(Array::size).allMatch(s -> s == size())) {
            throw new IllegalArgumentException("All columns must be the same size");
        }
    }

    @Override
    public final Iterator<Column<?>> iterator() {
        return new ColumnIterator(columns().entrySet().iterator());
    }

    @Override
    public final void forEach(Consumer<? super Column<?>> action) {
        for (Entry<String, Array<?>> e : columns().entrySet()) {
            action.accept(adapt(e));
        }
    }

    @Override
    public final Spliterator<Column<?>> spliterator() {
        return columns().entrySet().stream()
            .map((Function<Entry<String, Array<?>>, Column<?>>) NewTable::adapt).spliterator();
    }

    private static Column<?> adapt(Entry<String, Array<?>> e) {
        return Column.of(e.getKey(), e.getValue());
    }

    private static class ColumnIterator implements Iterator<Column<?>> {
        private final Iterator<Entry<String, Array<?>>> it;

        ColumnIterator(Iterator<Entry<String, Array<?>>> it) {
            this.it = Objects.requireNonNull(it);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Column<?> next() {
            return adapt(it.next());
        }
    }
}
