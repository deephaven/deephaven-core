package io.deephaven.qst.column.header;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static io.deephaven.qst.column.header.ColumnHeader.DEFAULT_BUILDER_INITIAL_CAPACITY;

@Immutable
@BuildableStyle
public abstract class ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9>
        implements TableHeader.Buildable {

    public abstract List<ColumnHeader<?>> headers();

    public abstract ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> others();

    public final ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> header(ColumnHeader<?> header) {
        return ImmutableColumnHeadersN.<T1, T2, T3, T4, T5, T6, T7, T8, T9>builder()
                .others(others()).addAllHeaders(headers()).addHeaders(header).build();
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h, T9 i,
            Object... remaining) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g, h, i, remaining);
    }

    @Check
    final void checkSize() {
        if (headers().isEmpty()) {
            throw new IllegalArgumentException(String
                    .format("Additional headers are empty, use %s instead", ColumnHeaders9.class));
        }
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9>.Rows others;
        private final List<ArrayBuilder<?, ?, ?>> builders;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builders = new ArrayList<>();
            for (ColumnHeader<?> header : headers()) {
                builders.add(Array.builder(header.componentType()));
            }
        }

        public final Rows row(T1 c1, T2 c2, T3 c3, T4 c4, T5 c5, T6 c6, T7 c7, T8 c8, T9 c9,
                Object... remaining) {
            if (remaining.length != headers().size()) {
                final int expected = 9 + headers().size();
                final int actual = 9 + remaining.length;
                throw new IllegalArgumentException(
                        String.format("Expected %d columns, found %d", expected, actual));
            }
            others.row(c1, c2, c3, c4, c5, c6, c7, c8, c9);
            int ix = 0;
            for (Object item : remaining) {
                // noinspection rawtypes
                ArrayBuilder builder = builders.get(ix);
                // noinspection unchecked
                builder.add(item);
                ++ix;
            }
            return this;
        }

        final List<Column<?>> list() {
            final List<Column<?>> cols = new ArrayList<>(9 + headers().size());
            for (Column<?> other : others) {
                cols.add(other);
            }
            int ix = 0;
            for (ColumnHeader<?> header : headers()) {
                // noinspection rawtypes
                Array array = builders.get(ix).build();
                // noinspection unchecked
                Column<?> c = Column.of(header.name(), array);
                cols.add(c);
                ++ix;
            }
            return cols;
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return list().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), headers().stream());
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
