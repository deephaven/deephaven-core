//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkboxer;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.Context;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Convert an arbitrary chunk to a chunk of boxed objects.
 */
public class ChunkBoxer {

    /**
     * Return a chunk that contains boxed {@link Object Objects} representing the primitive values in {@code values}.
     */
    public interface BoxerKernel extends Context {
        /**
         * Box all values into {@link Object Objects} if they are not already {@code Objects}.
         *
         * @param values the values to box
         *
         * @return a chunk containing values as {@code Objects} (not owned by the caller)
         */
        ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values);
    }

    /**
     * Box the value at {@code offset} in {@code values}.
     * <p>
     * Please use a {@link #getBoxer(ChunkType, int) ChunkBoxer} when boxing multiple values in order to amortize the
     * cost of implementation lookup and avoid virtual dispatch.
     *
     * @param values The chunk containing the value to box
     * @param offset The offset of the value to box
     * @return The boxed value
     * @param <BOXED_TYPE> The type of the boxed value
     */
    @SuppressWarnings("unchecked")
    public static <BOXED_TYPE> BOXED_TYPE boxedGet(@NotNull final Chunk<? extends Values> values, int offset) {
        final ChunkType type = values.getChunkType();
        switch (type) {
            case Boolean:
                return (BOXED_TYPE) Boolean.valueOf(values.asBooleanChunk().get(offset));
            case Char:
                return (BOXED_TYPE) TypeUtils.box(values.asCharChunk().get(offset));
            case Byte:
                return (BOXED_TYPE) TypeUtils.box(values.asByteChunk().get(offset));
            case Short:
                return (BOXED_TYPE) TypeUtils.box(values.asShortChunk().get(offset));
            case Int:
                return (BOXED_TYPE) TypeUtils.box(values.asIntChunk().get(offset));
            case Long:
                return (BOXED_TYPE) TypeUtils.box(values.asLongChunk().get(offset));
            case Float:
                return (BOXED_TYPE) TypeUtils.box(values.asFloatChunk().get(offset));
            case Double:
                return (BOXED_TYPE) TypeUtils.box(values.asDoubleChunk().get(offset));
            case Object:
                return (BOXED_TYPE) values.asObjectChunk().get(offset);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    public static BoxerKernel getBoxer(ChunkType type, int capacity) {
        switch (type) {
            case Boolean:
                return new BooleanBoxer(capacity);
            case Char:
                return new CharBoxer(capacity);
            case Byte:
                return new ByteBoxer(capacity);
            case Short:
                return new ShortBoxer(capacity);
            case Int:
                return new IntBoxer(capacity);
            case Long:
                return new LongBoxer(capacity);
            case Float:
                return new FloatBoxer(capacity);
            case Double:
                return new DoubleBoxer(capacity);
            case Object:
                return OBJECT_BOXER;
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    private static final ObjectBoxer OBJECT_BOXER = new ObjectBoxer();

    private static class ObjectBoxer implements BoxerKernel {
        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            return values.asObjectChunk();
        }
    }

    private static abstract class BoxerCommon implements ChunkBoxer.BoxerKernel {
        final WritableObjectChunk<Object, Values> objectChunk;

        private BoxerCommon(int capacity) {
            objectChunk = WritableObjectChunk.makeWritableChunk(capacity);
        }

        @Override
        public void close() {
            objectChunk.close();
        }
    }

    private static class BooleanBoxer extends BoxerCommon {
        BooleanBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final BooleanChunk<? extends Values> booleanChunk = values.asBooleanChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                // noinspection UnnecessaryBoxing
                objectChunk.set(ii, Boolean.valueOf(booleanChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class CharBoxer extends BoxerCommon {
        CharBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final CharChunk<? extends Values> charChunk = values.asCharChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(charChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class ByteBoxer extends BoxerCommon {
        ByteBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final ByteChunk<? extends Values> byteChunk = values.asByteChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(byteChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class ShortBoxer extends BoxerCommon {
        ShortBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final ShortChunk<? extends Values> shortChunk = values.asShortChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(shortChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class IntBoxer extends BoxerCommon {
        IntBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final IntChunk<? extends Values> intChunk = values.asIntChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(intChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class LongBoxer extends BoxerCommon {
        LongBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final LongChunk<? extends Values> longChunk = values.asLongChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(longChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class FloatBoxer extends BoxerCommon {
        FloatBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final FloatChunk<? extends Values> floatChunk = values.asFloatChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(floatChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }

    private static class DoubleBoxer extends BoxerCommon {
        DoubleBoxer(int capacity) {
            super(capacity);
        }

        @Override
        public ObjectChunk<?, ? extends Values> box(Chunk<? extends Values> values) {
            final DoubleChunk<? extends Values> doubleChunk = values.asDoubleChunk();
            for (int ii = 0; ii < values.size(); ++ii) {
                objectChunk.set(ii, TypeUtils.box(doubleChunk.get(ii)));
            }
            objectChunk.setSize(values.size());
            return objectChunk;
        }
    }
}
