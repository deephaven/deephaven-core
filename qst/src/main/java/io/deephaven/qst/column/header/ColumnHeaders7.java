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
public abstract class ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T7> header7();

    @Parameter
    public abstract ColumnHeaders6<T1, T2, T3, T4, T5, T6> others();

    public final <T8> ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> header(String name,
            Class<T8> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T8> ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> header(String name,
            Type<T8> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T8> ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> header(
            ColumnHeader<T8> header) {
        return ImmutableColumnHeaders8.of(header, this);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders6<T1, T2, T3, T4, T5, T6>.Rows others;
        private final ArrayBuilder<T7, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(header7().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b, T3 c, T4 d, T5 e, T6 f, T7 g) {
            others.row(a, b, c, d, e, f);
            builder.add(g);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T7> thisColumn = Column.of(header7().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), Stream.of(header7()));
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
