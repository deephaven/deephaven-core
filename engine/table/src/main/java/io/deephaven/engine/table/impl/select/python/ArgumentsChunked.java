/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.engine.util.PyCallableWrapper.ChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ColumnChunkArgument;
import io.deephaven.util.PrimitiveArrayType;

import java.util.Collection;
import java.util.Objects;

public class ArgumentsChunked {
    private final Collection<ChunkArgument> chunkArguments;
    private final Class<?>[] chunkedArgTypes;
    private final Object[] chunkedArgs;
    private final boolean forNumba;

    public void fillColumnChunks(int maxChunkSize) {
        int i = forNumba ? 0 : 1;
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof ColumnChunkArgument) {
                Class<?> argType = arg.getType();
                if (argType == byte.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.bytes().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.bytes().newInstance(maxChunkSize);
                } else if (argType == boolean.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.booleans().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.booleans().newInstance(maxChunkSize);
                } else if (argType == char.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.chars().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.chars().newInstance(maxChunkSize);
                } else if (argType == short.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.shorts().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.shorts().newInstance(maxChunkSize);
                } else if (argType == int.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.ints().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.ints().newInstance(maxChunkSize);
                } else if (argType == long.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.longs().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.longs().newInstance(maxChunkSize);
                } else if (argType == float.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.floats().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.floats().newInstance(maxChunkSize);
                } else if (argType == double.class) {
                    chunkedArgTypes[i] = PrimitiveArrayType.doubles().getArrayType();
                    chunkedArgs[i] = PrimitiveArrayType.doubles().newInstance(maxChunkSize);
                } else {
                    chunkedArgTypes[i] = Object[].class;
                    chunkedArgs[i] = new Object[maxChunkSize];
                }
            }
            i++;
        }
    }

    public void resolveColumnChunks(Chunk<?>[] chunkSources, int chunkSize) {
        // final Class<?>[] paramTypes = new Class[chunkSources.length];
        // final Object[] params = new Object[chunkSources.length];
        // for (int i = 0; i < chunkSources.length; i++) {
        // final ChunkToArray<?> cta = chunkSources[i].walk(new ChunkToArray<>());
        // paramTypes[i] = Objects.requireNonNull(cta.getArrayType());
        // params[i] = Objects.requireNonNull(cta.getArray());
        // }

        // for DH vectorized callable, we pass in the chunk size as the first argument
        if (!forNumba) {
            chunkedArgs[0] = chunkSize;
            chunkedArgTypes[0] = int.class;
        }

        int i = forNumba ? 0 : 1;
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof ColumnChunkArgument) {
                int idx = ((ColumnChunkArgument) arg).getChunkSourceIndex();
                if (chunkedArgTypes[i] == byte[].class) {
                    chunkSources[idx].asByteChunk().copyToTypedArray(0, (byte[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == boolean[].class) {
                    chunkSources[idx].asBooleanChunk().copyToTypedArray(0, (boolean[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == char[].class) {
                    chunkSources[idx].asCharChunk().copyToTypedArray(0, (char[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == short[].class) {
                    chunkSources[idx].asShortChunk().copyToTypedArray(0, (short[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == int[].class) {
                    chunkSources[idx].asIntChunk().copyToTypedArray(0, (int[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == long[].class) {
                    chunkSources[idx].asLongChunk().copyToTypedArray(0, (long[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == float[].class) {
                    chunkSources[idx].asFloatChunk().copyToTypedArray(0, (float[]) chunkedArgs[i], 0, chunkSize);
                } else if (chunkedArgTypes[i] == double[].class) {
                    chunkSources[idx].asDoubleChunk().copyToTypedArray(0, (double[]) chunkedArgs[i], 0, chunkSize);
                } else {
                    chunkSources[idx].asObjectChunk().copyToTypedArray(0, (Object[]) chunkedArgs[i], 0, chunkSize);
                }
            }
            i++;
        }
    }

    public ArgumentsChunked(Collection<ChunkArgument> chunkArguments, Object[] chunkedArgs, Class<?>[] argTypes,
            boolean numbaVectorized) {
        this.chunkArguments = chunkArguments;
        this.chunkedArgs = chunkedArgs;
        this.chunkedArgTypes = argTypes;
        this.forNumba = numbaVectorized;
    }

    public ArgumentsChunked clone() {
        return new ArgumentsChunked(chunkArguments, chunkedArgs.clone(), chunkedArgTypes.clone(), forNumba);
    }

    Class<?>[] getChunkedArgTypes() {
        return chunkedArgTypes;
    }

    Object[] getChunkedArgs() {
        return chunkedArgs;
    }

//    private static class ChunkToArray<ATTR extends Any> implements Chunk.Visitor<ATTR> {
//
//        private Class<?> arrayType;
//        private Object array;
//
//        Class<?> getArrayType() {
//            return Objects.requireNonNull(arrayType);
//        }
//
//        Object getArray() {
//            return Objects.requireNonNull(array);
//        }
//
//        @Override
//        public void visit(ByteChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.bytes().getArrayType();
//            final byte[] out = PrimitiveArrayType.bytes().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(BooleanChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.booleans().getArrayType();
//            final boolean[] out = PrimitiveArrayType.booleans().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(CharChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.chars().getArrayType();
//            final char[] out = PrimitiveArrayType.chars().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(ShortChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.shorts().getArrayType();
//            final short[] out = PrimitiveArrayType.shorts().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(IntChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.ints().getArrayType();
//            final int[] out = PrimitiveArrayType.ints().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(LongChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.longs().getArrayType();
//            final long[] out = PrimitiveArrayType.longs().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(FloatChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.floats().getArrayType();
//            final float[] out = PrimitiveArrayType.floats().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public void visit(DoubleChunk<ATTR> chunk) {
//            arrayType = PrimitiveArrayType.doubles().getArrayType();
//            final double[] out = PrimitiveArrayType.doubles().newInstance(chunk.size());
//            chunk.copyToTypedArray(0, out, 0, out.length);
//            array = out;
//        }
//
//        @Override
//        public <T> void visit(ObjectChunk<T, ATTR> chunk) {
//            // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to return
//            // the array type
//            arrayType = Object[].class;
//            final Object[] out = new Object[chunk.size()];
//            chunk.copyToArray(0, out, 0, out.length);
//            array = out;
//        }
//    }
}
