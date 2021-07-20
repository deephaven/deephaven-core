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
public abstract class ColumnHeaders5<A, B, C, D, E> {

    @Parameter
    public abstract ColumnHeader<E> headerE();

    @Parameter
    public abstract ColumnHeaders4<A, B, C, D> others();

    public final <F> ColumnHeaders6<A, B, C, D, E, F> header(String name, Class<F> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <F> ColumnHeaders6<A, B, C, D, E, F> header(String name, Type<F> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <F> ColumnHeaders6<A, B, C, D, E, F> header(ColumnHeader<F> header) {
        return ImmutableColumnHeaders6.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerE()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c, D d, E e) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders4<A, B, C, D>.Rows others;
        private final ArrayBuilder<E, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerE().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c, D d, E e) {
            others.row(a, b, c, d);
            builder.add(e);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<E> thisColumn = Column.of(headerE().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
