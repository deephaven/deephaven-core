package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.vector.ByteVectorDirect;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.vector.IntVectorDirect;
import io.deephaven.engine.table.lang.QueryLibrary;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FormulaTestUtil {
    static final int[] BASE_VALUES = new int[] {0, 65, QueryConstants.NULL_INT};
    static final int QUERYSCOPE_OBJ_BASE_VALUE = 42;
    /**
     * The index of the row (within {@link #BASE_VALUES} that contains zeroes.
     */
    static final int ZERO_ROW_INDEX = 0;
    /**
     * The index of the row (within {@link #BASE_VALUES} that contains null values.
     */
    static final int NULL_ROW_INDEX = 2;
    public static QueryScope pythonScope;

    /**
     * Cast {@code BASE_VALUES[index]} to the appropriate primitive type, then box it and return it.
     */
    @SuppressWarnings("RedundantCast")
    static Object getBoxedBaseVal(int index, Class<?> type) {
        final int baseVal = BASE_VALUES[index];
        if (baseVal == QueryConstants.NULL_INT) {
            return getNull(type);
        } else if (type == byte.class) {
            return (byte) baseVal;
        } else if (type == short.class) {
            return (short) baseVal;
        } else if (type == char.class) {
            return (char) baseVal;
        } else if (type == int.class) {
            return (int) baseVal;
        } else if (type == float.class) {
            return (float) baseVal;
        } else if (type == long.class) {
            return (long) baseVal;
        } else if (type == double.class) {
            return (double) baseVal;
        } else {
            throw new IllegalStateException("Unexpected type:" + type);
        }
    }

    static Object getNull(Class<?> type) {
        if (type == byte.class) {
            return QueryConstants.NULL_BYTE;
        } else if (type == short.class) {
            return QueryConstants.NULL_SHORT;
        } else if (type == char.class) {
            return QueryConstants.NULL_CHAR;
        } else if (type == int.class) {
            return QueryConstants.NULL_INT;
        } else if (type == float.class) {
            return QueryConstants.NULL_FLOAT;
        } else if (type == long.class) {
            return QueryConstants.NULL_LONG;
        } else if (type == double.class) {
            return QueryConstants.NULL_DOUBLE;
        } else {
            throw new IllegalStateException("Unexpected type:" + type);
        }
    }

    /**
     * Returns true if {@code t} is an exception of {@code exceptionType} or was caused by one.
     *
     * @param t The throwable to check. Cannot be null.
     * @param exceptionType The type to check against
     */
    static boolean involvesExceptionType(Throwable t, Class<? extends Exception> exceptionType) {
        Assert.neqNull(t, "t");
        for (; t != null; t = t.getCause()) {
            if (exceptionType.isAssignableFrom(t.getClass())) {
                return true;
            }
        }
        return false;
    }

    static void setUpQueryLibrary() {
        QueryLibrary.importPackage(Package.getPackage("java.util.concurrent"));

        QueryLibrary.importClass(Calendar.class);
        QueryLibrary.importClass(ArrayTypeUtils.class);

        QueryLibrary.importStatic(FormulaTestUtil.class);
    }

    static void setUpQueryScope() {
        QueryScope.addParam("myBoolean", true);
        QueryScope.addParam("myString", Integer.toString(QUERYSCOPE_OBJ_BASE_VALUE));
        QueryScope.addParam("myCharSequence", (CharSequence) Integer.toString(QUERYSCOPE_OBJ_BASE_VALUE));
        QueryScope.addParam("myObject", Object.class);

        QueryScope.addParam("myIntArray", BASE_VALUES.clone());
        QueryScope.addParam("myDoubleArray", IntStream.of(BASE_VALUES).asDoubleStream().toArray());
        QueryScope.addParam("myCharArray", ArrayTypeUtils.getUnboxedArray(
                IntStream.of(BASE_VALUES).mapToObj((anInt) -> (char) anInt).toArray(Character[]::new)));
        QueryScope.addParam("myDoubleObjArray",
                IntStream.of(BASE_VALUES).asDoubleStream().boxed().toArray(Double[]::new));
        QueryScope.addParam("myIntegerObjArray", IntStream.of(BASE_VALUES).boxed().toArray(Integer[]::new));
        QueryScope.addParam("myByteArray", IntStream.of(BASE_VALUES));

        QueryScope.addParam("myBooleanObj", true);
        QueryScope.addParam("myCharObj", (char) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myByteObj", (byte) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myShortObj", (short) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myIntObj", (int) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myLongObj", (long) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myFloatObj", (float) QUERYSCOPE_OBJ_BASE_VALUE);
        QueryScope.addParam("myDoubleObj", (double) QUERYSCOPE_OBJ_BASE_VALUE);

        QueryScope.addParam("myArrayList",
                new ArrayList<>(IntStream.of(BASE_VALUES).boxed().collect(Collectors.toList())));
        QueryScope.addParam("myHashMap",
                new HashMap<>(Collections.singletonMap(QUERYSCOPE_OBJ_BASE_VALUE, QUERYSCOPE_OBJ_BASE_VALUE)));
        QueryScope.addParam("myVector", new ObjectVectorDirect<>(IntStream.of(BASE_VALUES).boxed().toArray()));
        QueryScope.addParam("myEnumValue", TestFormulaColumnEnum.ONE);
        QueryScope.addParam("myObjectVector", ObjectVector.class);
        QueryScope.addParam("myIntVector", new IntVectorDirect(BASE_VALUES));
        QueryScope.addParam("myByteVector", new ByteVectorDirect(ArrayTypeUtils
                .getUnboxedArray(IntStream.of(BASE_VALUES).boxed().map(Integer::byteValue).toArray(Byte[]::new))));
        // QueryScope.addParam("myBooleanVector", BooleanVector.class);

        QueryScope.addParam("ExampleQuantity", 1);
        QueryScope.addParam("ExampleQuantity2", 2d);
        QueryScope.addParam("ExampleQuantity3", 3d);
        QueryScope.addParam("ExampleQuantity4", 4d);
        QueryScope.addParam("ExampleStr", String.class);
    }

    @NotNull
    static Table getTestDataTable() {
        return TableTools.newTable(
                TableTools.col("BooleanCol", false, true, QueryConstants.NULL_BOOLEAN),
                TableTools.charCol("CharCol", (char) BASE_VALUES[0], (char) BASE_VALUES[1], QueryConstants.NULL_CHAR),
                TableTools.byteCol("ByteCol", (byte) BASE_VALUES[0], (byte) BASE_VALUES[1], QueryConstants.NULL_BYTE),
                TableTools.shortCol("ShortCol", (short) BASE_VALUES[0], (short) BASE_VALUES[1],
                        QueryConstants.NULL_SHORT),
                TableTools.intCol("IntCol", (int) BASE_VALUES[0], (int) BASE_VALUES[1], QueryConstants.NULL_INT),
                TableTools.longCol("LongCol", (long) BASE_VALUES[0], (long) BASE_VALUES[1], QueryConstants.NULL_LONG),
                TableTools.floatCol("FloatCol", (float) BASE_VALUES[0], (float) BASE_VALUES[1],
                        QueryConstants.NULL_FLOAT),
                TableTools.doubleCol("DoubleCol", (double) BASE_VALUES[0], (double) BASE_VALUES[1],
                        QueryConstants.NULL_DOUBLE));
    }

    enum TestFormulaColumnEnum {
        ONE, TWO
    }
}
