/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.impl.select.Formula.FillContext;
import io.deephaven.engine.table.impl.select.FormulaKernelTypedBase;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import org.jpy.PyObject;

import java.util.Objects;

/**
 * A python formula kernel which is implemented by passing the chunks as arrays into the python function.
 *
 * @see io.deephaven.engine.table.impl.select.python.FormulaKernelPythonSingularFunction
 */
class FormulaKernelPythonChunkedFunction extends FormulaKernelTypedBase implements FormulaKernel {

    private static final String CALL_METHOD = "__call__";

    // this is a python function whose arguments can accept arrays
    private final PyObject function;
    private final ArgumentsChunked argumentsChunked;

    FormulaKernelPythonChunkedFunction(PyObject function, ArgumentsChunked argumentsChunked) {
        this.function = Objects.requireNonNull(function, "function");
        this.argumentsChunked = argumentsChunked;
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableByteChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final byte[] output = function
                .call(byte[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableBooleanChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final boolean[] output = function
                .call(boolean[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableCharChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final char[] output = function
                .call(char[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableShortChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final short[] output = function
                .call(short[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableIntChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final int[] output = function
                .call(int[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableLongChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final long[] output = function
                .call(long[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableFloatChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final float[] output = function
                .call(float[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableDoubleChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        final double[] output = function
                .call(double[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());
        __destination.copyFromTypedArray(output, 0, 0, __destination.size());
    }

    @Override
    public <T> void applyFormulaChunk(
            FillContext __context,
            WritableObjectChunk<T, ? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        FillContextPython fillContextPython = (FillContextPython) __context;
        fillContextPython.resolveColumnChunks(__sources, __destination.size());
        // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to return
        // the array type
        final Object[] output = function
                .call(Object[].class, CALL_METHOD, fillContextPython.getChunkedArgTypes(),
                        fillContextPython.getChunkedArgs());

        // noinspection unchecked
        __destination.copyFromTypedArray((T[]) output, 0, 0, __destination.size());
    }

    @Override
    public FillContext makeFillContext(int __chunkCapacity) {
        return argumentsChunked.makeFillContextPython(__chunkCapacity);
    }
}
