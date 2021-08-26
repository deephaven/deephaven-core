package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value.Check;
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

    private static final String DB_BOOLEAN_ARRAY = "io.deephaven.db.tables.dbarrays.DbBooleanArray";
    private static final String DB_BYTE_ARRAY = "io.deephaven.db.tables.dbarrays.DbByteArray";
    private static final String DB_CHAR_ARRAY = "io.deephaven.db.tables.dbarrays.DbCharArray";
    private static final String DB_SHORT_ARRAY = "io.deephaven.db.tables.dbarrays.DbShortArray";
    private static final String DB_INT_ARRAY = "io.deephaven.db.tables.dbarrays.DbIntArray";
    private static final String DB_LONG_ARRAY = "io.deephaven.db.tables.dbarrays.DbLongArray";
    private static final String DB_FLOAT_ARRAY = "io.deephaven.db.tables.dbarrays.DbFloatArray";
    private static final String DB_DOUBLE_ARRAY = "io.deephaven.db.tables.dbarrays.DbDoubleArray";
    private static final Set<String> VALID_CLASSES =
            Stream.of(DB_BOOLEAN_ARRAY, DB_BYTE_ARRAY, DB_CHAR_ARRAY, DB_SHORT_ARRAY, DB_INT_ARRAY,
                    DB_LONG_ARRAY, DB_FLOAT_ARRAY, DB_DOUBLE_ARRAY).collect(Collectors.toSet());

    public static <T, ComponentType> DbPrimitiveArrayType<T, ComponentType> of(Class<T> clazz,
            PrimitiveType<ComponentType> primitiveType) {
        return ImmutableDbPrimitiveArrayType.of(clazz, primitiveType);
    }

    static List<DbPrimitiveArrayType<?, ?>> types() throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        List<DbPrimitiveArrayType<?, ?>> out = new ArrayList<>(VALID_CLASSES.size());
        for (String className : VALID_CLASSES) {
            out.add(invokeTypeMethod(className));
        }
        return out;
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
    public abstract PrimitiveType<ComponentType> componentType();

    @Override
    public final <V extends ArrayType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkClazz() {
        if (!VALID_CLASSES.contains(clazz().getName())) {
            throw new IllegalArgumentException(String.format("Class '%s' is not a valid '%s'",
                    clazz(), DbPrimitiveArrayType.class));
        }
    }
}
