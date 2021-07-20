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
public abstract class ColumnHeaders6<A, B, C, D, E, F> {

    @Parameter
    public abstract ColumnHeader<F> headerF();

    @Parameter
    public abstract ColumnHeaders5<A, B, C, D, E> others();

    public final <G> ColumnHeaders7<A, B, C, D, E, F, G> header(String name, Class<G> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <G> ColumnHeaders7<A, B, C, D, E, F, G> header(String name, Type<G> type) {
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

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b, C c, D d, E e, F f) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b, c, d, e, f);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeaders5<A, B, C, D, E>.Rows others;
        private final ArrayBuilder<F, ?, ?> builder;

        Rows(int initialCapacity) {
            others = others().start(initialCapacity);
            builder = Array.builder(headerF().type(), initialCapacity);
        }

        public final Rows row(A a, B b, C c, D d, E e, F f) {
            others.row(a, b, c, d, e);
            builder.add(f);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<F> thisColumn = Column.of(headerF().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
