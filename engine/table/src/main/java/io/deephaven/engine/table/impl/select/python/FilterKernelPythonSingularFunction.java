package io.deephaven.engine.table.impl.select.python;

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
}
