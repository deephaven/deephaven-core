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
 * The primitive array types under the package {@code io.deephaven.vector}.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 */
@Immutable
@SimpleStyle
public abstract class PrimitiveVectorType<T, ComponentType>
        extends ArrayTypeBase<T, ComponentType> {

    private static final String BOOLEAN_VECTOR = "io.deephaven.vector.BooleanVector";
    private static final String BYTE_VECTOR = "io.deephaven.vector.ByteVector";
    private static final String CHAR_VECTOR = "io.deephaven.vector.CharVector";
    private static final String SHORT_VECTOR = "io.deephaven.vector.ShortVector";
    private static final String INT_VECTOR = "io.deephaven.vector.IntVector";
    private static final String LONG_VECTOR = "io.deephaven.vector.LongVector";
    private static final String FLOAT_VECTOR = "io.deephaven.vector.FloatVector";
    private static final String DOUBLE_VECTOR = "io.deephaven.vector.DoubleVector";
    private static final Set<String> VALID_CLASSES =
            Stream.of(BOOLEAN_VECTOR, BYTE_VECTOR, CHAR_VECTOR, SHORT_VECTOR, INT_VECTOR,
                    LONG_VECTOR, FLOAT_VECTOR, DOUBLE_VECTOR).collect(Collectors.toSet());

    public static <T, ComponentType> PrimitiveVectorType<T, ComponentType> of(Class<T> clazz,
            PrimitiveType<ComponentType> primitiveType) {
        return ImmutablePrimitiveVectorType.of(clazz, primitiveType);
    }

    static List<PrimitiveVectorType<?, ?>> types() throws ClassNotFoundException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        List<PrimitiveVectorType<?, ?>> out = new ArrayList<>(VALID_CLASSES.size());
        for (String className : VALID_CLASSES) {
            out.add(invokeTypeMethod(className));
        }
        return out;
    }

    private static <ComponentType> PrimitiveVectorType<?, ComponentType> invokeTypeMethod(
            String className) throws ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        final Class<?> clazz = Class.forName(className);
        final Method method = clazz.getDeclaredMethod("type");
        // noinspection rawtypes,unchecked
        return (PrimitiveVectorType) method.invoke(null);
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
                    clazz(), PrimitiveVectorType.class));
        }
    }
}
