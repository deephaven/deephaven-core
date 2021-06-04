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
public abstract class ColumnHeaders6<A, B, C, D, E, F> {

    @Parameter
    public abstract ColumnHeader<F> headerF();

    @Parameter
    public abstract ColumnHeaders5<A, B, C, D, E> others();

    public final <G> ColumnHeaders7<A, B, C, D, E, F, G> header(String name, Class<G> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <G> ColumnHeaders7<A, B, C, D, E, F, G> header(String name, ColumnType<G> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <G> ColumnHeaders7<A, B, C, D, E, F, G> header(ColumnHeader<G> header) {
        return ImmutableColumnHeaders7.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerF()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e, F f) {
        return start().row(a, b, c, d, e, f);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders5<A, B, C, D, E>.Rows others;
        private final ColumnBuilder<F> builder;

        Rows() {
            others = others().start();
            builder = Column.builder(headerF());
        }

        public final Rows row(A a, B b, C c, D d, E e, F f) {
            others.row(a, b, c, d, e);
            builder.add(f);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
