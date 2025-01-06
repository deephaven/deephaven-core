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
 * A python filter kernel which is implemented by iterating over the input chunks and calling the python function N
 * times.
 *
 * @see FilterKernelPythonChunkedFunction
 */
class FilterKernelPythonSingularFunction implements FilterKernel<FilterKernel.Context> {

    private static final String CALL_METHOD = "__call__";

    private final PyObject function;

    FilterKernelPythonSingularFunction(PyObject function) {
        this.function = Objects.requireNonNull(function, "function");
    }

    @Override
    public Context getContext(int maxChunkSize) {
        return new Context(maxChunkSize);
    }

    @Override
    public LongChunk<OrderedRowKeys> filter(
            Context context,
            LongChunk<OrderedRowKeys> indices,
            Chunk... inputChunks) {
        final int size = indices.size();
        final Class<?>[] paramTypes = ArgumentsSingular.buildParamTypes(inputChunks);
        context.resultChunk.setSize(0);
        for (int i = 0; i < size; ++i) {
            final Object[] params = ArgumentsSingular.buildArguments(inputChunks, i);
            if (function.call(boolean.class, CALL_METHOD, paramTypes, params)) {
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
        final Class<?>[] paramTypes = ArgumentsSingular.buildParamTypes(inputChunks);
        context.resultChunk.setSize(0);
        // Count the number of true values
        int count = 0;
        for (int i = 0; i < chunkSize; ++i) {
            final Object[] params = ArgumentsSingular.buildArguments(inputChunks, i);
            boolean newResult = function.call(boolean.class, CALL_METHOD, paramTypes, params);
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
        final Class<?>[] paramTypes = ArgumentsSingular.buildParamTypes(inputChunks);
        context.resultChunk.setSize(0);
        // Count the values that changed from true to false
        int count = 0;
        for (int i = 0; i < chunkSize; ++i) {
            boolean result = results.get(i);
            // Save the cost of the call if the result is already false
            if (!result) {
                continue;
            }
            final Object[] params = ArgumentsSingular.buildArguments(inputChunks, i);
            boolean newResult = function.call(boolean.class, CALL_METHOD, paramTypes, params);
            results.set(i, newResult);
            count += result == newResult ? 0 : 1;
        }
        return count;
    }
}
