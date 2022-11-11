/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.chunk.Chunk;
import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.util.PyCallableWrapper;
import io.deephaven.engine.util.PyCallableWrapper.ChunkArgument;

import java.util.Collection;
import java.util.Objects;

public class FillContextPython implements FillContext {
    private final Collection<ChunkArgument> chunkArguments;
    private final Object[] chunkedArgs;
    private final Class<?>[] chunkedArgTypes;
    private final boolean forNumba;

    public FillContextPython(Collection<ChunkArgument> chunkArguments, Object[] chunkedArgs, Class<?>[] chunkedArgTypes,
            boolean forNumba) {
        this.chunkArguments = chunkArguments;
        this.chunkedArgs = chunkedArgs;
        this.chunkedArgTypes = chunkedArgTypes;
        this.forNumba = forNumba;
    }

    public FillContextPython() {
        this.chunkArguments = null;
        this.chunkedArgs = null;
        this.chunkedArgTypes = null;
        this.forNumba = false;

    }

    public void resolveColumnChunks(Chunk<?>[] chunkSources, int chunkSize) {
        if (chunkArguments == null) {
            throw new IllegalStateException("Attempt to use the empty FillContextPython.");
        }
        // for DH vectorized callable, we pass in the chunk size as the first argument
        if (!forNumba) {
            chunkedArgs[0] = chunkSize;
            chunkedArgTypes[0] = int.class;
        }

        int i = forNumba ? 0 : 1;
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof PyCallableWrapper.ColumnChunkArgument) {
                int idx = ((PyCallableWrapper.ColumnChunkArgument) arg).getChunkSourceIndex();
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

    public Object[] getChunkedArgs() {
        return Objects.requireNonNull(chunkedArgs, "chunkedArgs");
    }

    public Class<?>[] getChunkedArgTypes() {
        return Objects.requireNonNull(chunkedArgTypes, "chunkedArgTypes");
    }

    @Override
    public void close() {
        // ignore
    }
}
