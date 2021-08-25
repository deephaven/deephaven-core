package io.deephaven.qst.column.header;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Iterator;
import java.util.stream.Stream;

import static io.deephaven.qst.column.header.ColumnHeader.DEFAULT_BUILDER_INITIAL_CAPACITY;

@Immutable
@SimpleStyle
public abstract class ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8>
        implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T8> header8();

    @Parameter
    public abstract ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> others();

    public final <T9> ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> header(String name,
            Class<T9> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T9> ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> header(String name,
            Type<T9> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T9> ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> header(
            ColumnHeader<T9> header) {
        return ImmutableColumnHeaders9.of(header, this);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g, h);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7>.Rows others;
        private final ArrayBuilder<T8, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(header8().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g, T8 h) {
            others.row(a, b, c, d, e, f, g);
            builder.add(h);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T8> thisColumn = Column.of(header8().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), Stream.of(header8()));
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
