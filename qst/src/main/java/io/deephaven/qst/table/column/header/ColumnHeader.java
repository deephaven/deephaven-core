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
public abstract class ColumnHeader<T> {

    public static <T> ColumnHeader<T> of(String name, Class<T> clazz) {
        return of(name, ColumnType.find(clazz));
    }

    public static <T> ColumnHeader<T> of(String name, ColumnType<T> type) {
        return ImmutableColumnHeader.of(name, type);
    }

    public static ColumnHeader<Boolean> ofBoolean(String name) {
        return of(name, ColumnType.booleanType());
    }

    public static ColumnHeader<Byte> ofByte(String name) {
        return of(name, ColumnType.byteType());
    }

    public static ColumnHeader<Character> ofChar(String name) {
        return of(name, ColumnType.charType());
    }

    public static ColumnHeader<Short> ofShort(String name) {
        return of(name, ColumnType.shortType());
    }

    public static ColumnHeader<Integer> ofInt(String name) {
        return of(name, ColumnType.intType());
    }

    public static ColumnHeader<Long> ofLong(String name) {
        return of(name, ColumnType.longType());
    }

    public static ColumnHeader<Float> ofFloat(String name) {
        return of(name, ColumnType.floatType());
    }

    public static ColumnHeader<Double> ofDouble(String name) {
        return of(name, ColumnType.doubleType());
    }

    public static ColumnHeader<String> ofString(String name) {
        return of(name, ColumnType.stringType());
    }

    public static <A, B> ColumnHeaders2<A, B> of(ColumnHeader<A> a, ColumnHeader<B> b) {
        return a.header(b);
    }

    public static <A, B, C> ColumnHeaders3<A, B, C> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c) {
        return a.header(b).header(c);
    }

    public static <A, B, C, D> ColumnHeaders4<A, B, C, D> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c, ColumnHeader<D> d) {
        return a.header(b).header(c).header(d);
    }

    public static <A, B, C, D, E> ColumnHeaders5<A, B, C, D, E> of(ColumnHeader<A> a, ColumnHeader<B> b, ColumnHeader<C> c, ColumnHeader<D> d, ColumnHeader<E> e) {
        return a.header(b).header(c).header(d).header(e);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract ColumnType<T> type();

    public final <B> ColumnHeaders2<T, B> header(String name, Class<B> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <B> ColumnHeaders2<T, B> header(String name, ColumnType<B> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <B> ColumnHeaders2<T, B> header(ColumnHeader<B> header) {
        return ImmutableColumnHeaders2.of(this, header);
    }

    public final ColumnHeader<T> headerA() {
        return this;
    }

    public final Stream<ColumnHeader<?>> headers() {
        return Stream.of(headerA());
    }

    public final TableHeader toTableHeader() {
        return TableHeader.of(() -> headers().iterator());
    }

    public final Column<T> emptyData() {
        return Column.empty(this);
    }

    public final Column<T> withData(T... data) {
        return Column.of(this, data);
    }

    public final Column<T> withData(Iterable<T> data) {
        return Column.of(this, data);
    }

    public final ColumnBuilder<T> columnBuilder() {
        return Column.builder(this);
    }

    public final Rows start() {
        return new Rows();
    }

    public final Rows row(T a) {
        return start().row(a);
    }

    public class Rows extends NewTableBuildable {

        private final ColumnBuilder<T> builder;

        Rows() {
            builder = Column.builder(ColumnHeader.this);
        }

        public final Rows row(T a) {
            builder.add(a);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            return Stream.of(builder.build());
        }
    }
}
