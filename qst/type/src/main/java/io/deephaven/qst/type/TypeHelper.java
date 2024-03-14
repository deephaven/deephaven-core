//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        return Stream.concat(PrimitiveType.instances(), genericTypes()).collect(Collectors.toList());
    }

    static Stream<GenericType<?>> genericTypes() {
        return Stream.of(
                BoxedType.instances(),
                Stream.of(StringType.of(), InstantType.of()),
                primitiveVectorTypes())
                .flatMap(Function.identity());
    }

    static Stream<PrimitiveVectorType<?, ?>> primitiveVectorTypes() {
        try {
            return PrimitiveVectorType.types().stream();
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException
                | IllegalAccessException e) {
            return Stream.empty();
        }
    }

    static <T> Optional<Type<T>> findStatic(Class<T> clazz) {
        // noinspection unchecked
        return Optional.ofNullable((Type<T>) MAPPINGS.get(clazz));
    }

    static class AddMappings implements Type.Visitor<Void>, GenericType.Visitor<Void> {

        private final Map<Class<?>, Type<?>> mappings = new HashMap<>();

        private <T> void add(Class<T> clazz, Type<T> type) {
            if (mappings.put(clazz, type) != null) {
                throw new IllegalStateException(String.format("Already added '%s'", clazz));
            }
        }

        private void addUnchecked(Class<?> clazz, Type<?> type) {
            if (mappings.put(clazz, type) != null) {
                throw new IllegalStateException(String.format("Already added '%s'", clazz));
            }
        }

        @Override
        public Void visit(PrimitiveType<?> primitiveType) {
            addUnchecked(primitiveType.clazz(), primitiveType);
            return null;
        }

        @Override
        public Void visit(GenericType<?> genericType) {
            genericType.walk((GenericType.Visitor<Void>) this);
            return null;
        }

        @Override
        public Void visit(BoxedType<?> boxedType) {
            addUnchecked(boxedType.clazz(), boxedType);
            return null;
        }

        @Override
        public Void visit(StringType stringType) {
            add(String.class, stringType);
            return null;
        }

        @Override
        public Void visit(InstantType instantType) {
            add(Instant.class, instantType);
            return null;
        }

        @Override
        public Void visit(ArrayType<?, ?> arrayType) {
            return arrayType.walk(new ArrayType.Visitor<Void>() {
                @Override
                public Void visit(NativeArrayType<?, ?> nativeArrayType) {
                    throw new IllegalArgumentException(
                            "Native array types should not be created statically, they will be found dynamically");
                }

                @Override
                public Void visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
                    addUnchecked(vectorPrimitiveType.clazz(), vectorPrimitiveType);
                    return null;
                }

                @Override
                public Void visit(GenericVectorType<?, ?> genericVectorType) {
                    // The engine array type by itself is not specific enough
                    throw new IllegalStateException(
                            "Should not be adding GenericVectorType as static mapping");
                }
            });
        }

        // NOTE: when adding new visitor methods, be sure to add the appropriate type to
        // knownTypes()

        @Override
        public Void visit(CustomType<?> customType) {
            throw new IllegalStateException("Should not be adding custom type as static mapping");
        }
    }
}
