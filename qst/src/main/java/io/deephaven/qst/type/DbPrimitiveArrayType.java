package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The primitive array types under the package {@code io.deephaven.db.tables.dbarrays}.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 */
@Immutable
@SimpleStyle
public abstract class DbPrimitiveArrayType<T, ComponentType>
    extends ArrayTypeBase<T, ComponentType> {

    public static <T, ComponentType> DbPrimitiveArrayType<T, ComponentType> of(Class<T> clazz,
        PrimitiveType<ComponentType> primitiveType) {
        return ImmutableDbPrimitiveArrayType.of(clazz, primitiveType);
    }

    public static DbPrimitiveArrayType<?, Boolean> booleanType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbBooleanArray");
    }

    public static DbPrimitiveArrayType<?, Byte> byteType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbByteArray");
    }

    public static DbPrimitiveArrayType<?, Character> charType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbCharArray");
    }

    public static DbPrimitiveArrayType<?, Short> shortType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbShortArray");
    }

    public static DbPrimitiveArrayType<?, Integer> intType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbIntArray");
    }

    public static DbPrimitiveArrayType<?, Long> longType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbLongArray");
    }

    public static DbPrimitiveArrayType<?, Float> floatType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbFloatArray");
    }

    public static DbPrimitiveArrayType<?, Double> doubleType() throws ClassNotFoundException,
        InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        return invokeTypeMethod("io.deephaven.db.tables.dbarrays.DbDoubleArray");
    }

    private static <ComponentType> DbPrimitiveArrayType<?, ComponentType> invokeTypeMethod(
        String className) throws ClassNotFoundException, NoSuchMethodException,
        InvocationTargetException, IllegalAccessException {
        final Class<?> clazz = Class.forName(className);
        final Method method = clazz.getDeclaredMethod("type");
        // noinspection rawtypes,unchecked
        return (DbPrimitiveArrayType) method.invoke(null);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract Type<ComponentType> componentType();

    @Override
    public final <V extends ArrayType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
