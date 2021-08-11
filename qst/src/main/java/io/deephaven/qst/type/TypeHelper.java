package io.deephaven.qst.type;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TypeHelper {
    private static final Map<Class<?>, Type<?>> MAPPINGS;

    static {
        final AddMappings addMappings = new AddMappings();
        for (Type<?> type : knownTypes()) {
            type.walk(addMappings);
        }
        MAPPINGS = Collections.unmodifiableMap(addMappings.mappings);
    }

    static List<Type<?>> knownTypes() {
        return Arrays.asList(BooleanType.instance(), ByteType.instance(), CharType.instance(),
            ShortType.instance(), IntType.instance(), LongType.instance(), FloatType.instance(),
            DoubleType.instance(), StringType.instance(), InstantType.instance());
    }

    static <T> Optional<Type<T>> findStatic(Class<T> clazz) {
        // noinspection unchecked
        return Optional.ofNullable((Type<T>) MAPPINGS.get(clazz));
    }

    static class AddMappings implements Type.Visitor, PrimitiveType.Visitor, GenericType.Visitor {

        private final Map<Class<?>, Type<?>> mappings = new HashMap<>();

        private <T> void add(Class<T> clazz, Type<T> type) {
            if (mappings.put(clazz, type) != null) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void visit(PrimitiveType<?> primitiveType) {
            primitiveType.walk((PrimitiveType.Visitor) this);
        }

        @Override
        public void visit(GenericType<?> genericType) {
            genericType.walk((GenericType.Visitor) this);
        }

        @Override
        public void visit(BooleanType booleanType) {
            add(boolean.class, booleanType);
            add(Boolean.class, booleanType);
        }

        @Override
        public void visit(ByteType byteType) {
            add(byte.class, byteType);
            add(Byte.class, byteType);
        }

        @Override
        public void visit(CharType charType) {
            add(char.class, charType);
            add(Character.class, charType);
        }

        @Override
        public void visit(ShortType shortType) {
            add(short.class, shortType);
            add(Short.class, shortType);
        }

        @Override
        public void visit(IntType intType) {
            add(int.class, intType);
            add(Integer.class, intType);
        }

        @Override
        public void visit(LongType longType) {
            add(long.class, longType);
            add(Long.class, longType);
        }

        @Override
        public void visit(FloatType floatType) {
            add(float.class, floatType);
            add(Float.class, floatType);
        }

        @Override
        public void visit(DoubleType doubleType) {
            add(double.class, doubleType);
            add(Double.class, doubleType);
        }

        @Override
        public void visit(StringType stringType) {
            add(String.class, stringType);
        }

        @Override
        public void visit(InstantType instantType) {
            add(Instant.class, instantType);
        }

        // NOTE: when adding new visitor methods, be sure to add the appropriate type to
        // knownTypes()

        @Override
        public void visit(CustomType<?> customType) {
            throw new IllegalStateException("Should not be adding custom type as static mapping");
        }
    }
}
