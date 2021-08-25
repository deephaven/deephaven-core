package io.deephaven.db.v2.select.python;

import io.deephaven.db.v2.select.ConditionFilter.FilterKernel;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
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
    public LongChunk<OrderedKeyIndices> filter(
            Context context,
            LongChunk<OrderedKeyIndices> indices,
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
