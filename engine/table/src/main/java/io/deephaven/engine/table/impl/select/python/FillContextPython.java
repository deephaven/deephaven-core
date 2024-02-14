/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.chunk.Chunk;
import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.util.PyCallableWrapper.ChunkArgument;
import io.deephaven.engine.util.PyCallableWrapperJpyImpl;

import java.util.Collection;
import java.util.Objects;

public class FillContextPython implements FillContext {

    public final static FillContextPython EMPTY = new FillContextPython();
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

    private FillContextPython() {
        this.chunkArguments = null;
        this.chunkedArgs = null;
        this.chunkedArgTypes = null;
        this.forNumba = false;
    }

    public void resolveColumnChunks(final Chunk<?>[] sourceChunks, final int chunkSize) {
        if (chunkArguments == null) {
            throw new IllegalStateException("Attempt to use the empty FillContextPython.");
        }
        // for DH vectorized callable, we pass in the chunk size as the first argument and the result array as the 2nd
        if (!forNumba) {
            chunkedArgs[0] = chunkSize;
            chunkedArgTypes[0] = int.class;
        }

        int argIndex = forNumba ? 0 : 2;
        for (ChunkArgument arg : chunkArguments) {
            if (arg instanceof PyCallableWrapperJpyImpl.ColumnChunkArgument) {
                final int sourceChunkIndex = ((PyCallableWrapperJpyImpl.ColumnChunkArgument) arg).getSourceChunkIndex();
                sourceChunks[sourceChunkIndex].copyToArray(0, chunkedArgs[argIndex], 0, chunkSize);
            }
            argIndex++;
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
