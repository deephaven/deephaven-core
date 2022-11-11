/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.chunk.*;
import io.deephaven.engine.util.PyCallableWrapper.ChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ColumnChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ConstantChunkArgument;
import io.deephaven.util.PrimitiveArrayType;

import java.util.Arrays;
import java.util.Collection;

public class ArgumentsChunked {
    private final Collection<ChunkArgument> chunkArguments;
    private final boolean forNumba;

    public FillContextPython makeFillContextPython(int maxChunkSize) {
        final Class<?>[] chunkedArgTypes;
        final Object[] chunkedArgs;

        // For DH vectorized, we add a parameter at the beginning for chunk size
        if (forNumba) {
            chunkedArgs = new Object[chunkArguments.size()];
            chunkedArgTypes = new Class[chunkArguments.size()];
        } else {
            // For DH vectorized func, we prepend 1 parameter to communicate the chunk size
            chunkedArgs = new Object[chunkArguments.size() + 1];
            chunkedArgTypes = new Class[chunkArguments.size() + 1];
        }

        int i = forNumba ? 0 : 1;
        for (ChunkArgument arg : chunkArguments) {
            Class<?> argType = arg.getType();
            if (argType == byte.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.bytes().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.bytes().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((byte[]) chunkedArgs[i], (byte) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == boolean.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.booleans().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.booleans().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((boolean[]) chunkedArgs[i], (boolean) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == char.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.chars().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.chars().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((char[]) chunkedArgs[i], (char) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == short.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.shorts().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.shorts().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((short[]) chunkedArgs[i], (short) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == int.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.ints().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.ints().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((int[]) chunkedArgs[i], (int) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == long.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.longs().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.longs().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((long[]) chunkedArgs[i], (long) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == float.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.floats().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.floats().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((float[]) chunkedArgs[i], (float) ((ConstantChunkArgument) arg).getValue());
                }
            } else if (argType == double.class) {
                chunkedArgTypes[i] = PrimitiveArrayType.doubles().getArrayType();
                chunkedArgs[i] = PrimitiveArrayType.doubles().newInstance(maxChunkSize);
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((double[]) chunkedArgs[i], (double) ((ConstantChunkArgument) arg).getValue());
                }
            } else {
                chunkedArgTypes[i] = Object[].class;
                chunkedArgs[i] = new Object[maxChunkSize];
                if (arg instanceof ConstantChunkArgument) {
                    Arrays.fill((Object[]) chunkedArgs[i], ((ConstantChunkArgument) arg).getValue());
                }
            }
            i++;
        }
        return new FillContextPython(chunkArguments, chunkedArgs, chunkedArgTypes, forNumba);
    }

    public ArgumentsChunked(Collection<ChunkArgument> chunkArguments, boolean forNumba) {
        this.chunkArguments = chunkArguments;
        this.forNumba = forNumba;
    }
}
