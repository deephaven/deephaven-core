package io.deephaven.qst.table;

import io.deephaven.qst.table.ImmutableTableHeader.Builder;
import io.deephaven.qst.table.column.Column;
import io.deephaven.qst.table.column.header.ColumnHeader;
import java.util.Iterator;
import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class NewTable extends TableBase {

    public static NewTable empty(TableHeader header) {
        ImmutableNewTable.Builder builder = ImmutableNewTable.builder().size(0);
        for (ColumnHeader<?> columnHeader : header.headers()) {
            builder.addColumns(columnHeader.emptyData());
        }
        return builder.build();
    }

    public static NewTable of(Column<?>... columns) {
        final int size = columns.length > 0 ? columns[0].size() : 0;
        return ImmutableNewTable.builder().addColumns(columns).size(size).build();
    }

    public static NewTable of(Iterable<Column<?>> columns) {
        Iterator<Column<?>> it = columns.iterator();
        final int size = it.hasNext() ? it.next().size() : 0;
        return ImmutableNewTable.builder().addAllColumns(columns).size(size).build();
    }

    public static <A> ColumnHeader<A> header(String name, Class<A> clazz) {
        return ColumnHeader.of(name, clazz);
    }

    public abstract List<Column<?>> columns();

    public abstract int size();

    public final int numColumns() {
        return columns().size();
    }

    public final TableHeader header() {
        Builder builder = ImmutableTableHeader.builder();
        for (Column<?> column : columns()) {
          builder.addHeaders(column.header());
        }
        return builder.build();
    }

    public final NewTable with(Column<?> column) {
        return ImmutableNewTable.builder()
            .size(size())
            .addAllColumns(columns())
            .addColumns(column)
            .build();
    }

    @Check
    final void checkColumnsSizes() {
        if (!columns()
            .stream()
            .map(Column::values)
            .mapToInt(List::size)
            .allMatch(s -> s == size())) {
            throw new IllegalArgumentException("All columns must be the same size");
        }
    }

    @Check
    final void checkValidHeader() {
        // ensure we can build the header
        if (header().numColumns() != numColumns()) {
            throw new IllegalArgumentException("All headers must have distinct names");
        }
    }
}
