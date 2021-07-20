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

import java.time.Instant;
import java.util.stream.Stream;

@Immutable
@SimpleStyle
public abstract class ColumnHeader<T> {

    static final int BUILDER_INITIAL_CAPACITY = 16;

    public static <T> ColumnHeader<T> of(String name, Class<T> clazz) {
        return of(name, Type.find(clazz));
    }

    public static <T> ColumnHeader<T> of(String name, Type<T> type) {
        return ImmutableColumnHeader.of(name, type);
    }

    public static ColumnHeader<Boolean> ofBoolean(String name) {
        return of(name, Type.booleanType());
    }

    public static ColumnHeader<Byte> ofByte(String name) {
        return of(name, Type.byteType());
    }

    public static ColumnHeader<Character> ofChar(String name) {
        return of(name, Type.charType());
    }

    public static ColumnHeader<Short> ofShort(String name) {
        return of(name, Type.shortType());
    }

    public static ColumnHeader<Integer> ofInt(String name) {
        return of(name, Type.intType());
    }

    public static ColumnHeader<Long> ofLong(String name) {
        return of(name, Type.longType());
    }

    public static ColumnHeader<Float> ofFloat(String name) {
        return of(name, Type.floatType());
    }

    public static ColumnHeader<Double> ofDouble(String name) {
        return of(name, Type.doubleType());
    }

    public static ColumnHeader<String> ofString(String name) {
        return of(name, Type.stringType());
    }

    public static ColumnHeader<Instant> ofInstant(String name) {
        return of(name, Type.instantType());
    }

    public static <A, B> ColumnHeaders2<A, B> of(ColumnHeader<A> a, ColumnHeader<B> b) {
        return a.header(b);
    }

    public static <A, B, C> ColumnHeaders3<A, B, C> of(ColumnHeader<A> a, ColumnHeader<B> b,
        ColumnHeader<C> c) {
        return a.header(b).header(c);
    }

    public static <A, B, C, D> ColumnHeaders4<A, B, C, D> of(ColumnHeader<A> a, ColumnHeader<B> b,
        ColumnHeader<C> c, ColumnHeader<D> d) {
        return a.header(b).header(c).header(d);
    }

    public static <A, B, C, D, E> ColumnHeaders5<A, B, C, D, E> of(ColumnHeader<A> a,
        ColumnHeader<B> b, ColumnHeader<C> c, ColumnHeader<D> d, ColumnHeader<E> e) {
        return a.header(b).header(c).header(d).header(e);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Type<T> type();

    public final <B> ColumnHeaders2<T, B> header(String name, Class<B> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <B> ColumnHeaders2<T, B> header(String name, Type<B> type) {
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

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T a) {
        return start(BUILDER_INITIAL_CAPACITY).row(a);
    }

    public class Rows extends NewTableBuildable {

        private final ArrayBuilder<T, ?, ?> arrayBuilder;

        Rows(int initialCapacity) {
            arrayBuilder = Array.builder(type(), initialCapacity);
        }

        public final Rows row(T a) {
            arrayBuilder.add(a);
            return this;
        }

        @Override
        protected final Stream<Column<?>> columns() {
            Column<T> thisColumn = Column.of(name(), arrayBuilder.build());
            return Stream.of(thisColumn);
        }
    }
}
