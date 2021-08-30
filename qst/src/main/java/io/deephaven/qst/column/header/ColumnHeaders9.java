package io.deephaven.qst.column.header;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Iterator;
import java.util.stream.Stream;

import static io.deephaven.qst.column.header.ColumnHeader.DEFAULT_BUILDER_INITIAL_CAPACITY;

@Immutable
@SimpleStyle
public abstract class ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9>
    implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T9> header9();

    @Parameter
    public abstract ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> others();

    public final ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> header(ColumnHeader<?> header) {
        return ImmutableColumnHeadersN.<T1, T2, T3, T4, T5, T6, T7, T8, T9>builder().others(this)
            .addHeaders(header).build();
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h, T9 i) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g, h, i);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8>.Rows others;
        private final ArrayBuilder<T9, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(header9().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h, T9 i) {
            others.row(a, b, c, d, e, f, g, h);
            builder.add(i);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T9> thisColumn = Column.of(header9().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), Stream.of(header9()));
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
