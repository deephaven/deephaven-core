package io.deephaven.qst.column.header;

import io.deephaven.qst.SimpleStyle;
import io.deephaven.qst.table.NewTableBuildable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.stream.Stream;

import static io.deephaven.qst.column.header.ColumnHeader.BUILDER_INITIAL_CAPACITY;

@Immutable
@SimpleStyle
public abstract class ColumnHeaders4<A, B, C, D> {

    @Parameter
    public abstract ColumnHeader<D> headerD();

    @Parameter
    public abstract ColumnHeaders3<A, B, C> others();

    public final <E> ColumnHeaders5<A, B, C, D, E> header(String name, Class<E> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <E> ColumnHeaders5<A, B, C, D, E> header(String name, Type<E> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <E> ColumnHeaders5<A, B, C, D, E> header(ColumnHeader<E> header) {
        return ImmutableColumnHeaders5.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerD()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c, D d) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c, d);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders3<A, B, C>.Rows others;
        private final ArrayBuilder<D, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerD().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c, D d) {
            others.row(a, b, c);
            builder.add(d);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<D> thisColumn = Column.of(headerD().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
