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
        public final Vector vectorWrap(@NotNull final Object array) {
            throw new UnsupportedOperationException("Vector is not implemented for primitive booleans");
        }

        @Override
        @NotNull
        public Vector vectorWrap(@NotNull final Object array, int offset, int capacity) {
            throw new UnsupportedOperationException("Vector is not implemented for primitive booleans");
        }
    },

    Char() {
        @Override
        @NotNull
        public final CharVectorDirect vectorWrap(@NotNull final Object array) {
            return new CharVectorDirect((char[]) array);
        }

        @Override
        @NotNull
        public CharVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new CharVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Byte() {
        @Override
        @NotNull
        public final ByteVectorDirect vectorWrap(@NotNull final Object array) {
            return new ByteVectorDirect((byte[]) array);
        }

        @Override
        @NotNull
        public ByteVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new ByteVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Short() {
        @Override
        @NotNull
        public final ShortVectorDirect vectorWrap(@NotNull final Object array) {
            return new ShortVectorDirect((short[]) array);
        }

        @Override
        @NotNull
        public ShortVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new ShortVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Int() {
        @Override
        @NotNull
        public final IntVectorDirect vectorWrap(@NotNull final Object array) {
            return new IntVectorDirect((int[]) array);
        }

        @Override
        @NotNull
        public IntVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new IntVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Long() {
        @Override
        @NotNull
        public final LongVectorDirect vectorWrap(@NotNull final Object array) {
            return new LongVectorDirect((long[]) array);
        }

        @Override
        @NotNull
        public LongVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new LongVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Float() {
        @Override
        @NotNull
        public final FloatVectorDirect vectorWrap(@NotNull final Object array) {
            return new FloatVectorDirect((float[]) array);
        }

        @Override
        @NotNull
        public FloatVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new FloatVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Double() {
        @Override
        @NotNull
        public final DoubleVectorDirect vectorWrap(@NotNull final Object array) {
            return new DoubleVectorDirect((double[]) array);
        }

        @Override
        @NotNull
        public DoubleVectorSlice vectorWrap(@NotNull final Object array, int offset, int capacity) {
            return new DoubleVectorSlice(vectorWrap(array), offset, capacity);
        }
    },

    Object() {
        @Override
        @NotNull
        public final ObjectVectorDirect<?> vectorWrap(@NotNull final Object array) {
            //noinspection unchecked
            return new ObjectVectorDirect((Object[]) array);
        }

        @Override
        @NotNull
        public ObjectVectorSlice<?> vectorWrap(@NotNull final Object array, int offset, int capacity) {
            //noinspection unchecked
            return new ObjectVectorSlice(vectorWrap(array), offset, capacity);
        }
    };

    // @formatter:on

    private static final SimpleTypeMap<VectorFactory> BY_ELEMENT_TYPE = SimpleTypeMap.create(
            VectorFactory.Boolean, VectorFactory.Char, VectorFactory.Byte, VectorFactory.Short, VectorFactory.Int,
            VectorFactory.Long, VectorFactory.Float, VectorFactory.Double, VectorFactory.Object);

    public static VectorFactory forElementType(@NotNull final Class<?> clazz) {
        return BY_ELEMENT_TYPE.get(clazz);
    }

    @NotNull
    public abstract Vector vectorWrap(@NotNull Object array);

    @NotNull
    public abstract Vector vectorWrap(@NotNull Object array, int offset, int capacity);
}
