package io.deephaven.engine.table.impl.sources;

import io.deephaven.time.DateTime;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import org.junit.Test;

import java.time.Instant;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayBackedColumnSourceTest {

    @Test
    public void fromBooleanArray() {
        check(Type.booleanType(), false, null, true);
    }

    @Test
    public void fromByteArray() {
        check(Type.byteType(), (byte) 1, null, (byte) 3);
    }

    @Test
    public void fromCharArray() {
        check(Type.charType(), '1', null, '3');
    }

    @Test
    public void fromShortArray() {
        check(Type.shortType(), (short) 1, null, (short) 3);
    }

    @Test
    public void fromIntArray() {
        check(Type.intType(), 1, null, 3);
    }

    @Test
    public void fromLongArray() {
        check(Type.longType(), 1L, null, 3L);
    }

    @Test
    public void fromFloatArray() {
        check(Type.floatType(), 1f, null, 3f);
    }

    @Test
    public void fromDoubleArray() {
        check(Type.doubleType(), 1d, null, 3d);
    }

    @Test
    public void fromStringArray() {
        check(Type.stringType(), "1", null, "3");
    }

    @Test
    public void fromInstants() {
        check(ArrayBackedColumnSourceTest::checkInstant, Type.instantType(), Instant.ofEpochMilli(1), null,
                Instant.ofEpochMilli(3));
    }

    @Test
    public void fromCustomType() {
        check(CustomType.of(MyCustomType.class), MyCustomType.A, null, MyCustomType.B);
    }

    enum MyCustomType {
        A, B
    }

    private static <T> void check(PrimitiveType<T> type, T... values) {
        PrimitiveArray<T> array = PrimitiveArray.of(type, values);
        ArrayBackedColumnSource<T> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).isEqualTo(left);
        }
        check(Objects::equals, type, values);
    }

    private static <T> void check(GenericType<T> type, T... values) {
        check(Objects::equals, type, values);
    }

    private static <T> void check(BiPredicate<T, Object> comparison, GenericType<T> type, T... values) {
        GenericArray<T> array = GenericArray.of(type, values);
        ArrayBackedColumnSource<?> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).matches((Predicate<Object>) right -> comparison.test(left, right));
        }
        check(comparison, (Type<T>) type, values);
    }

    private static <T> void check(BiPredicate<T, Object> comparison, Type<T> type, T... values) {
        Array<T> array = Array.of(type, values);
        ArrayBackedColumnSource<?> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).matches((Predicate<Object>) right -> comparison.test(left, right));
        }
    }

    private static boolean checkInstant(Instant instant, Object o) {
        return (instant == null && o == null) ||
                (instant != null && (o instanceof DateTime)
                        && instant.toEpochMilli() == ((DateTime) o).getMillis());
    }
}
