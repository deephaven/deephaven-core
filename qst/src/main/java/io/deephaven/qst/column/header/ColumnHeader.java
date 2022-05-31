package io.deephaven.qst.column.header;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.ArrayBuilder;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A column header is {@link #name() name} and {@link #componentType() type} pair.
 *
 * <p>
 * Multiple column headers, up to 9, can be strongly-linked together to provide a convenient interface for building
 * {@link NewTable new tables} in a row-oriented, type-safe manner.
 *
 * @param <T1> the type
 */
@Immutable
@SimpleStyle
public abstract class ColumnHeader<T1> implements TableHeader.Buildable {

    static final int DEFAULT_BUILDER_INITIAL_CAPACITY = 16;

    public static <T> ColumnHeader<T> of(String name, Class<T> componentType) {
        return of(name, Type.find(componentType));
    }

    public static <T> ColumnHeader<T> of(String name, Type<T> componentType) {
        return ImmutableColumnHeader.of(name, componentType);
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

    public static <T1, T2> ColumnHeaders2<T1, T2> of(ColumnHeader<T1> c1, ColumnHeader<T2> c2) {
        return c1.header(c2);
    }

    public static <T1, T2, T3> ColumnHeaders3<T1, T2, T3> of(ColumnHeader<T1> c1,
            ColumnHeader<T2> c2, ColumnHeader<T3> c3) {
        return of(c1, c2).header(c3);
    }

    public static <T1, T2, T3, T4> ColumnHeaders4<T1, T2, T3, T4> of(ColumnHeader<T1> c1,
            ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4) {
        return of(c1, c2, c3).header(c4);
    }

    public static <T1, T2, T3, T4, T5> ColumnHeaders5<T1, T2, T3, T4, T5> of(ColumnHeader<T1> c1,
            ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4, ColumnHeader<T5> c5) {
        return of(c1, c2, c3, c4).header(c5);
    }

    public static <T1, T2, T3, T4, T5, T6> ColumnHeaders6<T1, T2, T3, T4, T5, T6> of(
            ColumnHeader<T1> c1, ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4,
            ColumnHeader<T5> c5, ColumnHeader<T6> c6) {
        return of(c1, c2, c3, c4, c5).header(c6);
    }

    public static <T1, T2, T3, T4, T5, T6, T7> ColumnHeaders7<T1, T2, T3, T4, T5, T6, T7> of(
            ColumnHeader<T1> c1, ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4,
            ColumnHeader<T5> c5, ColumnHeader<T6> c6, ColumnHeader<T7> c7) {
        return of(c1, c2, c3, c4, c5, c6).header(c7);
    }

    public static <T1, T2, T3, T4, T5, T6, T7, T8> ColumnHeaders8<T1, T2, T3, T4, T5, T6, T7, T8> of(
            ColumnHeader<T1> c1, ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4,
            ColumnHeader<T5> c5, ColumnHeader<T6> c6, ColumnHeader<T7> c7, ColumnHeader<T8> c8) {
        return of(c1, c2, c3, c4, c5, c6, c7).header(c8);
    }

    public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> of(
            ColumnHeader<T1> c1, ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4,
            ColumnHeader<T5> c5, ColumnHeader<T6> c6, ColumnHeader<T7> c7, ColumnHeader<T8> c8,
            ColumnHeader<T9> c9) {
        return of(c1, c2, c3, c4, c5, c6, c7, c8).header(c9);
    }

    // Note: we can add additional typed ColumnHeaders about 9 if desired

    public static <T1, T2, T3, T4, T5, T6, T7, T8, T9> ColumnHeadersN<T1, T2, T3, T4, T5, T6, T7, T8, T9> of(
            ColumnHeader<T1> c1, ColumnHeader<T2> c2, ColumnHeader<T3> c3, ColumnHeader<T4> c4,
            ColumnHeader<T5> c5, ColumnHeader<T6> c6, ColumnHeader<T7> c7, ColumnHeader<T8> c8,
            ColumnHeader<T9> c9, ColumnHeader<?>... headers) {
        ColumnHeaders9<T1, T2, T3, T4, T5, T6, T7, T8, T9> typed =
                of(c1, c2, c3, c4, c5, c6, c7, c8, c9);
        return ImmutableColumnHeadersN.<T1, T2, T3, T4, T5, T6, T7, T8, T9>builder().others(typed)
                .addHeaders(headers).build();
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract Type<T1> componentType();

    public final <T2> ColumnHeaders2<T1, T2> header(String name, Class<T2> clazz) {
        return header(ColumnHeader.of(name, clazz));
    }

    public final <T2> ColumnHeaders2<T1, T2> header(String name, Type<T2> type) {
        return header(ColumnHeader.of(name, type));
    }

    public final <T2> ColumnHeaders2<T1, T2> header(ColumnHeader<T2> header) {
        return ImmutableColumnHeaders2.of(this, header);
    }

    public final Rows start(int initialCapacity) {
        return new Rows(initialCapacity);
    }

    public final Rows row(T1 a) {
        return start(DEFAULT_BUILDER_INITIAL_CAPACITY).row(a);
    }

    public class Rows implements NewTable.Buildable {

        private final ArrayBuilder<T1, ?, ?> arrayBuilder;

        Rows(int initialCapacity) {
            arrayBuilder = Array.builder(componentType(), initialCapacity);
        }

        public final Rows row(T1 a) {
            arrayBuilder.add(a);
            return this;
        }

        final Stream<Column<?>> stream() {
            Column<T1> thisColumn = Column.of(name(), arrayBuilder.build());
            return Stream.of(thisColumn);
        }

        @Override
        public final Iterator<Column<?>> iterator() {
            return stream().iterator();
        }
    }

    @Override
    public final Iterator<ColumnHeader<?>> iterator() {
        Set<ColumnHeader<?>> singleton = Collections.singleton(this);
        return singleton.iterator();
    }

    @Check
    void checkName() {
        NameValidator.isValidColumnName(name());
    }
}
