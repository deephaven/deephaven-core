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
public abstract class ColumnHeaders2<A, B> {

    @Parameter
    public abstract ColumnHeader<A> headerA();

    @Parameter
    public abstract ColumnHeader<B> headerB();

    public final <C> ColumnHeaders3<A, B, C> header(String name, Class<C> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <C> ColumnHeaders3<A, B, C> header(String name, Type<C> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <C> ColumnHeaders3<A, B, C> header(ColumnHeader<C> header) {
        return ImmutableColumnHeaders3.of(header, this);
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.of(headerA(), headerB());
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(A a, B b) {
        return start(BUILDER_INITIAL_CAPACITY).row(a, b);
    }

    public class Rows extends NewTableBuildable {
        private final ColumnHeader<A>.Rows others;
        private final ArrayBuilder<B, ?, ?> builder;

        Rows(int initialCapacity) {
            others = headerA().start(initialCapacity);
            builder = Array.builder(headerB().type(), initialCapacity);
        }

        public final Rows row(A a, B b) {
            others.row(a);
            builder.add(b);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<B> thisColumn = Column.of(headerB().name(), builder.build());
            return Stream.concat(others.columns(), Stream.of(thisColumn));
        }
    }
}
