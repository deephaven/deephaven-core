//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.util.PyCallableWrapper.ChunkArgument;
import io.deephaven.engine.util.PyCallableWrapper.ConstantChunkArgument;
import org.jpy.PyObject;

import java.util.Arrays;
import java.util.Collection;

public class ArgumentsChunked {
    private final Collection<ChunkArgument> chunkArguments;
    private final Class<?> returnType;
    private final boolean forNumba;

    private void prepareOneChunkedArg(Object[] chunkedArgs, Class<?>[] chunkedArgTypes, int argIdx, Class<?> argType,
            Object argValue, int maxChunkSize) {
        if (argType == byte.class) {
            chunkedArgTypes[argIdx] = byte[].class;
            chunkedArgs[argIdx] = new byte[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((byte[]) chunkedArgs[argIdx], (byte) argValue);
            }
        } else if (argType == boolean.class) {
            chunkedArgTypes[argIdx] = boolean[].class;
            chunkedArgs[argIdx] = new boolean[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((boolean[]) chunkedArgs[argIdx], (boolean) argValue);
            }
        } else if (argType == char.class) {
            chunkedArgTypes[argIdx] = char[].class;
            chunkedArgs[argIdx] = new char[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((char[]) chunkedArgs[argIdx], (char) argValue);
            }
        } else if (argType == short.class) {
            chunkedArgTypes[argIdx] = short[].class;
            chunkedArgs[argIdx] = new short[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((short[]) chunkedArgs[argIdx], (short) argValue);
            }
        } else if (argType == int.class) {
            chunkedArgTypes[argIdx] = int[].class;
            chunkedArgs[argIdx] = new int[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((int[]) chunkedArgs[argIdx], (int) argValue);
            }
        } else if (argType == long.class) {
            chunkedArgTypes[argIdx] = long[].class;
            chunkedArgs[argIdx] = new long[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((long[]) chunkedArgs[argIdx], (long) argValue);
            }
        } else if (argType == float.class) {
            chunkedArgTypes[argIdx] = float[].class;
            chunkedArgs[argIdx] = new float[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((float[]) chunkedArgs[argIdx], (float) argValue);
            }
        } else if (argType == double.class) {
            chunkedArgTypes[argIdx] = double[].class;
            chunkedArgs[argIdx] = new double[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((double[]) chunkedArgs[argIdx], (double) argValue);
            }
        } else if (argType == PyObject.class) {
            chunkedArgTypes[argIdx] = PyObject[].class;
            chunkedArgs[argIdx] = new PyObject[maxChunkSize];
            if (argValue != null) {
                Arrays.fill((PyObject[]) chunkedArgs[argIdx], (PyObject) argValue);
            }
        } else {
            chunkedArgTypes[argIdx] = Object[].class;
            chunkedArgs[argIdx] = new Object[maxChunkSize];
            Arrays.fill((Object[]) chunkedArgs[argIdx], argValue);
        }
    }

    public FillContextPython makeFillContextPython(int maxChunkSize) {
        final Class<?>[] chunkedArgTypes;
        final Object[] chunkedArgs;

        if (forNumba) {
            chunkedArgs = new Object[chunkArguments.size()];
            chunkedArgTypes = new Class[chunkArguments.size()];
        } else {
            // For DH vectorized func, we prepend 2 parameter to pass the chunk size and the return array
            chunkedArgs = new Object[chunkArguments.size() + 2];
            chunkedArgTypes = new Class[chunkArguments.size() + 2];
            prepareOneChunkedArg(chunkedArgs, chunkedArgTypes, 1, returnType, null, maxChunkSize);
        }

        int i = forNumba ? 0 : 2;
        for (ChunkArgument arg : chunkArguments) {
            Object argValue = arg instanceof ConstantChunkArgument ? ((ConstantChunkArgument) arg).getValue() : null;
            Class<?> argType = arg.getType();
            prepareOneChunkedArg(chunkedArgs, chunkedArgTypes, i, argType, argValue, maxChunkSize);
            i++;
        }
        return new FillContextPython(chunkArguments, chunkedArgs, chunkedArgTypes, forNumba);
    }

    public ArgumentsChunked(Collection<ChunkArgument> chunkArguments, Class<?> returnType, boolean forNumba) {
        this.chunkArguments = chunkArguments;
        this.returnType = returnType;
        this.forNumba = forNumba;
    }
}
