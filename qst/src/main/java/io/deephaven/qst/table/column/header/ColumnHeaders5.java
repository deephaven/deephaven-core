package io.deephaven.qst.table.column.header;

import io.deephaven.qst.table.NewTableBuildable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.column.Column;
import io.deephaven.qst.table.column.ColumnBuilder;
import io.deephaven.qst.table.column.type.ColumnType;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders5<A, B, C, D, E> {

    @Parameter
    public abstract ColumnHeader<E> headerE();

    @Parameter
    public abstract ColumnHeaders4<A, B, C, D> others();

    public final <F> ColumnHeaders6<A, B, C, D, E, F> header(String name, Class<F> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <F> ColumnHeaders6<A, B, C, D, E, F> header(String name, ColumnType<F> type) {
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

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e) {
        return start().row(a, b, c, d, e);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders4<A, B, C, D>.Rows others;
        private final ColumnBuilder<E> builder;

        Rows() {
            others = others().start();
            builder = Column.builder(headerE());
        }

        public final Rows row(A a, B b, C c, D d, E e) {
            others.row(a, b, c, d);
            builder.add(e);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
