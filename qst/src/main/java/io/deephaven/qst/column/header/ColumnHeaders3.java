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
public abstract class ColumnHeaders3<T1, T2, T3> implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T3> header3();

    @Parameter
    public abstract ColumnHeaders2<T1, T2> others();

    public final <T4> ColumnHeaders4<T1, T2, T3, T4> header(String name, Class<T4> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T4> ColumnHeaders4<T1, T2, T3, T4> header(String name, Type<T4> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T4> ColumnHeaders4<T1, T2, T3, T4> header(ColumnHeader<T4> header) {
        return ImmutableColumnHeaders4.of(header, this);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders2<T1, T2>.Rows others;
        private final ArrayBuilder<T3, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(header3().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b, T3 c) {
            others.row(a, b);
            builder.add(c);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T3> thisColumn = Column.of(header3().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), Stream.of(header3()));
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
