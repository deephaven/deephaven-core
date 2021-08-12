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
public abstract class ColumnHeaders2<T1, T2> implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T1> header1();

    @Parameter
    public abstract ColumnHeader<T2> header2();

    public final <T3> ColumnHeaders3<T1, T2, T3> header(String name, Class<T3> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T3> ColumnHeaders3<T1, T2, T3> header(String name, Type<T3> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T3> ColumnHeaders3<T1, T2, T3> header(ColumnHeader<T3> header) {
        return ImmutableColumnHeaders3.of(header, this);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeader<T1>.Rows others;
        private final ArrayBuilder<T2, ?, ?> builder;

        Rows(int initialCapacity) {
            others = header1().start(initialCapacity);
            builder = Array.builder(header2().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b) {
            others.row(a);
            builder.add(b);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T2> thisColumn = Column.of(header2().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.of(header1(), header2());
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
