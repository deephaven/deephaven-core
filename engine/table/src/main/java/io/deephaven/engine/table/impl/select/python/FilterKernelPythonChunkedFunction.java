//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import org.jpy.PyObject;

import java.util.Objects;

/**
 * A python filter kernel which is implemented by passing the chunks as arrays into the python function.
 *
 * @see io.deephaven.engine.table.impl.select.python.FilterKernelPythonSingularFunction
 */
class FilterKernelPythonChunkedFunction implements FilterKernel<FilterKernel.Context> {

    private static final String CALL_METHOD = "__call__";

    // this is a python function whose arguments can accept arrays
    private final PyObject function;

    private final ArgumentsChunked argumentsChunked;

    FilterKernelPythonChunkedFunction(PyObject function, ArgumentsChunked argumentsChunked) {
        this.function = Objects.requireNonNull(function, "function");
        this.argumentsChunked = argumentsChunked;
    }

    @Override
    public Context getContext(int maxChunkSize) {
        return new Context(maxChunkSize, argumentsChunked.makeFillContextPython(maxChunkSize));
    }

    @Override
    public LongChunk<OrderedRowKeys> filter(
            Context context,
            LongChunk<OrderedRowKeys> indices,
            Chunk... inputChunks) {
        final int size = indices.size();
        FillContextPython fillContextPython = context.getKernelContext();
        fillContextPython.resolveColumnChunks(inputChunks, size);

        final boolean[] results = function
                .call(boolean[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        if (size > results.length) {
            throw new IllegalStateException(
                    "FilterKernelPythonChunkedFunction returned results are not the proper size");
        }
        context.resultChunk.setSize(0);
        for (int i = 0; i < size; ++i) {
            if (results[i]) {
                context.resultChunk.add(indices.get(i));
            }
        }
        return context.resultChunk;
    }

    @Override
    public int filter(
            final Context context,
            final Chunk[] inputChunks,
            final int chunkSize,
            final WritableBooleanChunk<Values> results) {
        FillContextPython fillContextPython = context.getKernelContext();
        fillContextPython.resolveColumnChunks(inputChunks, chunkSize);

        final boolean[] pyResults = function
                .call(boolean[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        if (chunkSize > pyResults.length) {
            throw new IllegalStateException(
                    "FilterKernelPythonChunkedFunction returned results are not the proper size");
        }
        // Count the number of true values
        int count = 0;
        for (int i = 0; i < chunkSize; ++i) {
            boolean newResult = pyResults[i];
            results.set(i, newResult);
            count += newResult ? 1 : 0;
        }
        return count;
    }

    @Override
    public int filterAnd(
            final Context context,
            final Chunk[] inputChunks,
            final int chunkSize,
            final WritableBooleanChunk<Values> results) {
        FillContextPython fillContextPython = context.getKernelContext();
        fillContextPython.resolveColumnChunks(inputChunks, chunkSize);

        final boolean[] pyResults = function
                .call(boolean[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        if (chunkSize > pyResults.length) {
            throw new IllegalStateException(
                    "FilterKernelPythonChunkedFunction returned results are not the proper size");
        }
        // Count values that changed from true to false
        int count = 0;
        for (int i = 0; i < chunkSize; ++i) {
            boolean result = results.get(i);
            boolean newResult = result & pyResults[i];
            results.set(i, newResult);
            count += result == newResult ? 0 : 1;
        }
        return count;
    }
}
