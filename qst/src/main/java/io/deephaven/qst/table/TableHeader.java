package io.deephaven.qst.table;

import io.deephaven.qst.table.column.header.ColumnHeader;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class TableHeader implements Iterable<ColumnHeader<?>> {

    public static TableHeader empty() {
        return ImmutableTableHeader.builder().build();
    }

    public static TableHeader of(ColumnHeader<?>... headers) {
        return ImmutableTableHeader.builder().addHeaders(headers).build();
    }

    public static TableHeader of(Iterable<ColumnHeader<?>> headers) {
        return ImmutableTableHeader.builder().addAllHeaders(headers).build();
    }

    public abstract List<ColumnHeader<?>> headers();

    public final int numColumns() {
        return headers().size();
    }

    @Check
    final void checkDistinctColumnNames() {
        if (headers().size() != headers().stream().map(ColumnHeader::name).distinct().count()) {
            throw new IllegalArgumentException("All headers must have distinct names");
        }
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return headers().iterator();
    }

    @Override
    public final void forEach(Consumer<? super ColumnHeader<?>> action) {
        headers().forEach(action);
    }

    @Override
    public final Spliterator<ColumnHeader<?>> spliterator() {
        return headers().spliterator();
    }
}
