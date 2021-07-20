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
public abstract class ColumnHeaders7<A, B, C, D, E, F, G> {

    @Parameter
    public abstract ColumnHeader<G> headerG();

    @Parameter
    public abstract ColumnHeaders6<A, B, C, D, E, F> others();

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(String name, Class<H> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(String name, Type<H> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <H> ColumnHeaders8<A, B, C, D, E, F, G, H> header(ColumnHeader<H> header) {
        return ImmutableColumnHeaders8.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerG()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c, D d, E e, F f, G g) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders6<A, B, C, D, E, F>.Rows others;
        private final ArrayBuilder<G, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerG().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c, D d, E e, F f, G g) {
            others.row(a, b, c, d, e, f);
            builder.add(g);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<G> thisColumn = Column.of(headerG().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
