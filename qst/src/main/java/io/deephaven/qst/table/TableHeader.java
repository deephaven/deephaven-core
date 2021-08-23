package io.deephaven.qst.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A table header is a list of {@link ColumnHeader column headers}. Each column header will have a
 * distinct name.
 */
@Immutable
@BuildableStyle
public abstract class TableHeader implements Iterable<ColumnHeader<?>> {

    public interface Builder {
        Builder putHeaders(String key, Type<?> value);

        default Builder addHeaders(ColumnHeader<?> header) {
            return putHeaders(header.name(), header.componentType());
        }

        default Builder addHeaders(ColumnHeader<?>... headers) {
            for (ColumnHeader<?> header : headers) {
                addHeaders(header);
            }
            return this;
        }

        default Builder addAllHeaders(Iterable<ColumnHeader<?>> headers) {
            for (ColumnHeader<?> header : headers) {
                addHeaders(header);
            }
            return this;
        }

        TableHeader build();
    }

    public interface Buildable extends Iterable<ColumnHeader<?>> {

        default TableHeader tableHeader() {
            return builder().addAllHeaders(this).build();
        }
    }

    public static Builder builder() {
        return ImmutableTableHeader.builder();
    }

    public static TableHeader empty() {
        return builder().build();
    }

    public static TableHeader of(ColumnHeader<?>... headers) {
        return builder().addHeaders(headers).build();
    }

    public static TableHeader of(Iterable<ColumnHeader<?>> headers) {
        return builder().addAllHeaders(headers).build();
    }

    abstract Map<String, Type<?>> headers();

    public final int numColumns() {
        return headers().size();
    }

    public final Type<?> getHeader(String name) {
        return headers().get(name);
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return new ColumnHeaderIterator(headers().entrySet().iterator());
    }

    @Override
    public final void forEach(Consumer<? super ColumnHeader<?>> action) {
        for (Entry<String, Type<?>> e : headers().entrySet()) {
            action.accept(adapt(e));
        }
    }

    @Override
    public final Spliterator<ColumnHeader<?>> spliterator() {
        return headers().entrySet().stream()
            .map((Function<Entry<String, Type<?>>, ColumnHeader<?>>) TableHeader::adapt)
            .spliterator();
    }

    private static ColumnHeader<?> adapt(Entry<String, Type<?>> e) {
        return ColumnHeader.of(e.getKey(), e.getValue());
    }

    private static class ColumnHeaderIterator implements Iterator<ColumnHeader<?>> {
        private final Iterator<Entry<String, Type<?>>> it;

        ColumnHeaderIterator(Iterator<Entry<String, Type<?>>> it) {
            this.it = Objects.requireNonNull(it);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public ColumnHeader<?> next() {
            return adapt(it.next());
        }
    }
}
