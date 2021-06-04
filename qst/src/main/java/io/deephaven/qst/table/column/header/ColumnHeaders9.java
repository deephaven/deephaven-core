package io.deephaven.qst.table.column.header;

import io.deephaven.qst.table.NewTableBuildable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.table.column.Column;
import io.deephaven.qst.table.column.ColumnBuilder;
import java.util.stream.Stream;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnHeaders9<A, B, C, D, E, F, G, H, I> {

    @Parameter
    public abstract ColumnHeader<I> headerI();

    @Parameter
    public abstract ColumnHeaders8<A, B, C, D, E, F, G, H> others();

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerI()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h, I i) {
        return start().row(a, b, c, d, e, f, g, h, i);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders8<A, B, C, D, E, F, G, H>.Rows others;
        private final ColumnBuilder<I> builder;

        Rows() {
            others = others().start();
            builder = Column.builder(headerI());
        }

        public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h, I i) {
            others.row(a, b, c, d, e, f, g, h);
            builder.add(i);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.concat(others.columns(), Stream.of(builder.build()));
        }
    }
}
