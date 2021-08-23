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
public abstract class ColumnHeaders4<T1, T2, T3, T4> implements TableHeader.Buildable {

    @Parameter
    public abstract ColumnHeader<T4> header4();

    @Parameter
    public abstract ColumnHeaders3<T1, T2, T3> others();

    public final <T5> ColumnHeaders5<T1, T2, T3, T4, T5> header(String name, Class<T5> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T5> ColumnHeaders5<T1, T2, T3, T4, T5> header(String name, Type<T5> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T5> ColumnHeaders5<T1, T2, T3, T4, T5> header(ColumnHeader<T5> header) {
        return ImmutableColumnHeaders5.of(header, this);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a, T2 b, T3 c, T4 d) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a, b, c, d);
    }

    public class Rows implements NewTable.Buildable {
        private final ColumnHeaders3<T1, T2, T3>.Rows others;
        private final ArrayBuilder<T4, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(header4().componentType(), initialCapacity);
        }

        public final Rows row(T1 a, T2 b, T3 c, T4 d) {
            others.row(a, b, c);
            builder.add(d);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T4> thisColumn = Column.of(header4().name(), builder.build());
            return Stream.concat(others.stream(), Stream.of(thisColumn));
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    final Stream<ColumnHeader<?>> stream() {
        return Stream.concat(others().stream(), Stream.of(header4()));
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        return stream().iterator();
    }
}
