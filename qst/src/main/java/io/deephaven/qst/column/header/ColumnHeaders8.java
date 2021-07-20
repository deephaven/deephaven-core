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
public abstract class ColumnHeaders8<A, B, C, D, E, F, G, H> {

    @Parameter
    public abstract ColumnHeader<H> headerH();

    @Parameter
    public abstract ColumnHeaders7<A, B, C, D, E, F, G> others();

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(String name, Class<I> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(String name, Type<I> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <I> ColumnHeaders9<A, B, C, D, E, F, G, H, I> header(ColumnHeader<I> header) {
        return ImmutableColumnHeaders9.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.concat(others().headers(), Stream.of(headerH()));
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f, g, h);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders7<A, B, C, D, E, F, G>.Rows others;
        private final ArrayBuilder<H, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerH().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c, D d, E e, F f, G g, H h) {
            others.row(a, b, c, d, e, f, g);
            builder.add(h);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<H> thisColumn = Column.of(headerH().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
