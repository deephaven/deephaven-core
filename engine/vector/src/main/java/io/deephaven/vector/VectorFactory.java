/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.util.SimpleTypeMap;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for wrapping native arrays as {@link Vector} instances.
 */
public enum VectorFactory {

    // @formatter:off

    Boolean() {
         @Override
         @NotNull
         public Class<? extends Vector<?>> vectorType() {
             throw new UnsupportedOperationException("Vector is not implemented for primitive booleans");
         }

        @Override
        @NotNull
        public final Vector<?> vectorWrap(@NotNull final Object array) {
            throw new UnsupportedOperationException("Vector is not implemented for primitive booleans");
        }

        @Override
        @NotNull
        public Vector<?> vectorWrap(@NotNull final Object array, int offset, int capacity) {
            throw new UnsupportedOperationException("Vector is not implemented for primitive booleans");
        }
    },

    Char() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return CharVector.class;
        }

        @Override
        @NotNull
        public final CharVector vectorWrap(@NotNull final Object array) {
            return new CharVectorDirect((char[]) array);
        }

        @Override
        @NotNull
        public CharVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new CharVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Byte() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return ByteVector.class;
        }

        @Override
        @NotNull
        public final ByteVector vectorWrap(@NotNull final Object array) {
            return new ByteVectorDirect((byte[]) array);
        }

        @Override
        @NotNull
        public ByteVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new ByteVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Short() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return ShortVector.class;
        }

        @Override
        @NotNull
        public final ShortVector vectorWrap(@NotNull final Object array) {
            return new ShortVectorDirect((short[]) array);
        }

        @Override
        @NotNull
        public ShortVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new ShortVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Int() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return IntVector.class;
        }

        @Override
        @NotNull
        public final IntVector vectorWrap(@NotNull final Object array) {
            return new IntVectorDirect((int[]) array);
        }

        @Override
        @NotNull
        public IntVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new IntVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Long() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return LongVector.class;
        }

        @Override
        @NotNull
        public final LongVector vectorWrap(@NotNull final Object array) {
            return new LongVectorDirect((long[]) array);
        }

        @Override
        @NotNull
        public LongVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new LongVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Float() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return FloatVector.class;
        }

        @Override
        @NotNull
        public final FloatVector vectorWrap(@NotNull final Object array) {
            return new FloatVectorDirect((float[]) array);
        }

        @Override
        @NotNull
        public FloatVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new FloatVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Double() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            return DoubleVector.class;
        }

        @Override
        @NotNull
        public final DoubleVector vectorWrap(@NotNull final Object array) {
            return new DoubleVectorDirect((double[]) array);
        }

        @Override
        @NotNull
        public DoubleVector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new DoubleVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Object() {
        @Override
        @NotNull
        public Class<? extends Vector<?>> vectorType() {
            //noinspection unchecked
            return (Class<? extends Vector<?>>) (Object) ObjectVector.class;
        }

        @Override
        @NotNull
        public final ObjectVector<?> vectorWrap(@NotNull final Object array) {
            return new ObjectVectorDirect<>((Object[]) array);
        }

        @Override
        @NotNull
        public ObjectVector<?> vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new ObjectVectorSlice<>(vectorWrap(array), offset, capacity);
        }
    };

    // @formatter:on

    private static final SimpleTypeMap<VectorFactory> BY_ELEMENT_TYPE = SimpleTypeMap.create(
            VectorFactory.Boolean, VectorFactory.Char, VectorFactory.Byte, VectorFactory.Short, VectorFactory.Int,
            VectorFactory.Long, VectorFactory.Float, VectorFactory.Double, VectorFactory.Object);

    public static VectorFactory forElementType(@NotNull final Class<?> clazz) {
        return BY_ELEMENT_TYPE.get(clazz);
    }

    public abstract @NotNull Class<? extends Vector<?>> vectorType();

    @NotNull
    public abstract Vector<?> vectorWrap(@NotNull Object array);

    @NotNull
    public abstract Vector<?> vectorWrap(@NotNull Object array, int offset, int capacity);
}
