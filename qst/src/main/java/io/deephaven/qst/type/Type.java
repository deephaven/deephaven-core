package io.deephaven.qst.type;

import java.util.stream.Stream;

public interface Type<T> {

    static <T> Type<T> find(Class<T> clazz) {
        return KnownColumnTypes.findStatic(clazz).orElseGet(() -> CustomType.of(clazz));
    }

    static BooleanType booleanType() {
        return BooleanType.instance();
    }

    static ByteType byteType() {
        return ByteType.instance();
    }

    static CharType charType() {
        return CharType.instance();
    }

    static ShortType shortType() {
        return ShortType.instance();
    }

    static IntType intType() {
        return IntType.instance();
    }

    static LongType longType() {
        return LongType.instance();
    }

    static FloatType floatType() {
        return FloatType.instance();
    }

    static DoubleType doubleType() {
        return DoubleType.instance();
    }

    static StringType stringType() {
        return StringType.instance();
    }

    static InstantType instantType() {
        return InstantType.instance();
    }

    static <T> CustomType<T> ofCustom(Class<T> clazz) {
        return CustomType.of(clazz);
    }

    static Stream<Type<?>> knownTypes() {
        return Stream.of(booleanType(), byteType(), charType(), shortType(), intType(), longType(),
            floatType(), doubleType(), stringType(), instantType());
    }

    static <T> T castValue(@SuppressWarnings("unused") Type<T> columnType, Object value) {
        // noinspection unchecked
        return (T) value;
    }

    <V extends Visitor> V walk(V visitor);

    T castValue(Object value);

    interface Visitor {
        void visit(PrimitiveType<?> primitiveType);

        void visit(GenericType<?> genericType);
    }
}
