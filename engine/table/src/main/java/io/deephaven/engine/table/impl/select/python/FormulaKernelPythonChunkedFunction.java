/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.table.impl.select.FormulaKernelTypedBase;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.util.PythonScopeJpyImpl.CallableWrapper;

import java.util.Objects;

/**
 * A python formula kernel which is implemented by passing the chunks as arrays into the python function.
 *
 * @see io.deephaven.engine.table.impl.select.python.FormulaKernelPythonSingularFunction
 */
class FormulaKernelPythonChunkedFunction extends FormulaKernelTypedBase implements FormulaKernel {

    private static final String CALL_METHOD = "__call__";

    // this is a python function whose arguments can accept arrays
    private final CallableWrapper callableWrapper;

    FormulaKernelPythonChunkedFunction(CallableWrapper callableWrapper) {
        this.callableWrapper = Objects.requireNonNull(callableWrapper, "function");
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableByteChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final byte[] output = callableWrapper.getPyObject()
                .call(byte[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableBooleanChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final boolean[] output = callableWrapper.getPyObject()
                .call(boolean[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableCharChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final char[] output = callableWrapper.getPyObject()
                .call(char[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableShortChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final short[] output = callableWrapper.getPyObject()
                .call(short[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableIntChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final int[] output = callableWrapper.getPyObject()
                .call(int[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableLongChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final long[] output = callableWrapper.getPyObject()
                .call(long[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableFloatChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final float[] output = callableWrapper.getPyObject()
                .call(float[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableDoubleChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);
        final double[] output = callableWrapper.getPyObject()
                .call(double[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public <T> void applyFormulaChunk(
            FillContext __context,
            WritableObjectChunk<T, ? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final ArgumentsChunked args =
                ArgumentsChunked.buildArguments(__sources, callableWrapper);

        // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to return
        // the array type
        final Object[] output = callableWrapper.getPyObject()
                .call(Object[].class, CALL_METHOD, args.getParamTypes(), args.getParams());

        // noinspection unchecked
        __destination.copyFromTypedArray((T[]) output, 0, 0, __destination.size());
    }

    @Override
    public FillContext makeFillContext(int __chunkCapacity) {
        return FillContextPython.EMPTY;
    }
}
