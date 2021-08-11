package io.deephaven.db.v2.utils.unboxer;

import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;

/**
 * Convert an Object chunk to a chunk of primitives.
 */
public class ChunkUnboxer {
    static final CharUnboxer CHAR_EMPTY_INSTANCE = new CharUnboxer(0);
    static final ByteUnboxer BYTE_EMPTY_INSTANCE = new ByteUnboxer(0);
    static final ShortUnboxer SHORT_EMPTY_INSTANCE = new ShortUnboxer(0);
    static final IntUnboxer INT_EMPTY_INSTANCE = new IntUnboxer(0);
    static final LongUnboxer LONG_EMPTY_INSTANCE = new LongUnboxer(0);
    static final FloatUnboxer FLOAT_EMPTY_INSTANCE = new FloatUnboxer(0);
    static final DoubleUnboxer DOUBLE_EMPTY_INSTANCE = new DoubleUnboxer(0);

    /**
     * Return a chunk that contains boxed Objects representing the primitive values in primitives.
     */
    public interface UnboxerKernel extends Context {
        /**
         * Convert all boxed primitives to real primitives.
         *
         * @param boxedPrimitives the boxed primitives to convert
         *
         * @return a chunk containing primitives
         */
        Chunk<? extends Values> unbox(ObjectChunk<?, ? extends Values> boxedPrimitives);

        void unboxTo(ObjectChunk<?, ? extends Values> boxedPrimitives, WritableChunk<? extends Values> primitives, int sourceOffset, int destOffset);
    }

    public static UnboxerKernel getUnboxer(ChunkType type, int capacity) {
        if (capacity == 0) {
            return getEmptyUnboxer(type);
        }
        switch (type) {
            case Char:
                return new CharUnboxer(capacity);
            case Byte:
                return new ByteUnboxer(capacity);
            case Short:
                return new ShortUnboxer(capacity);
            case Int:
                return new IntUnboxer(capacity);
            case Long:
                return new LongUnboxer(capacity);
            case Float:
                return new FloatUnboxer(capacity);
            case Double:
                return new DoubleUnboxer(capacity);
            case Boolean:
            case Object:
                throw new IllegalArgumentException("Can not unbox objects");
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    public static UnboxerKernel getEmptyUnboxer(ChunkType type) {
        switch (type) {
            case Char:
                return CHAR_EMPTY_INSTANCE;
            case Byte:
                return BYTE_EMPTY_INSTANCE;
            case Short:
                return SHORT_EMPTY_INSTANCE;
            case Int:
                return INT_EMPTY_INSTANCE;
            case Long:
                return LONG_EMPTY_INSTANCE;
            case Float:
                return FLOAT_EMPTY_INSTANCE;
            case Double:
                return DOUBLE_EMPTY_INSTANCE;
            case Boolean:
            case Object:
                throw new IllegalArgumentException("Can not unbox objects");
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
