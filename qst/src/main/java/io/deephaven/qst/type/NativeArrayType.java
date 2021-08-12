package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.lang.reflect.Array;
import java.util.Objects;

/**
 * A java native array type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 */
@Immutable
@SimpleStyle
public abstract class NativeArrayType<T, ComponentType> extends ArrayTypeBase<T, ComponentType> {

    public static <T, ComponentType> NativeArrayType<T, ComponentType> of(Class<T> arrayType,
        Type<ComponentType> componentType) {
        return ImmutableNativeArrayType.of(arrayType, componentType);
    }

    public static <ComponentType> NativeArrayType<?, ComponentType> toArrayType(
        PrimitiveType<ComponentType> type) {
        return type.walk(new Adapter<ComponentType>()).out();
    }

    public static <ComponentType> NativeArrayType<?, ComponentType> toArrayType(
        GenericType<ComponentType> type) {
        // Note: in Java 12+, we can use Class#arrayType()
        final Class<?> clazz = Array.newInstance(type.clazz(), 0).getClass();
        return NativeArrayType.of(clazz, type);
    }

    public static NativeArrayType<boolean[], Boolean> booleanArrayType() {
        return of(boolean[].class, BooleanType.instance());
    }

    public static NativeArrayType<byte[], Byte> byteArrayType() {
        return of(byte[].class, ByteType.instance());
    }

    public static NativeArrayType<char[], Character> charArrayType() {
        return of(char[].class, CharType.instance());
    }

    public static NativeArrayType<short[], Short> shortArrayType() {
        return of(short[].class, ShortType.instance());
    }

    public static NativeArrayType<int[], Integer> intArrayType() {
        return of(int[].class, IntType.instance());
    }

    public static NativeArrayType<long[], Long> longArrayType() {
        return of(long[].class, LongType.instance());
    }

    public static NativeArrayType<float[], Float> floatArrayType() {
        return of(float[].class, FloatType.instance());
    }

    public static NativeArrayType<double[], Double> doubleArrayType() {
        return of(double[].class, DoubleType.instance());
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

    @Check
    final void checkArrayType() {
        if (!clazz().isArray()) {
            throw new IllegalArgumentException("clazz() must be an array type");
        }
        Class<?> componentType = clazz().getComponentType();
        if (!componentType.equals(componentType().clazz())) {
            throw new IllegalArgumentException("Component types don't match");
        }
    }

    static class Adapter<ComponentType> implements PrimitiveType.Visitor {

        private NativeArrayType<?, ?> out;

        public NativeArrayType<?, ComponentType> out() {
            // noinspection unchecked
            return (NativeArrayType<?, ComponentType>) Objects.requireNonNull(out);
        }

        @Override
        public void visit(BooleanType booleanType) {
            out = booleanArrayType();
        }

        @Override
        public void visit(ByteType byteType) {
            out = byteArrayType();
        }

        @Override
        public void visit(CharType charType) {
            out = charArrayType();
        }

        @Override
        public void visit(ShortType shortType) {
            out = shortArrayType();
        }

        @Override
        public void visit(IntType intType) {
            out = intArrayType();
        }

        @Override
        public void visit(LongType longType) {
            out = longArrayType();
        }

        @Override
        public void visit(FloatType floatType) {
            out = floatArrayType();
        }

        @Override
        public void visit(DoubleType doubleType) {
            out = doubleArrayType();
        }
    }
}
