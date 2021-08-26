package io.deephaven.db.v2.select.python;

import io.deephaven.db.v2.select.Formula.FillContext;
import io.deephaven.db.v2.select.FormulaKernelTypedBase;
import io.deephaven.db.v2.select.formula.FormulaKernel;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jpy.PyObject;

import java.util.Objects;

/**
 * A python formula kernel which is implemented by passing the chunks as arrays into the python function.
 *
 * @see io.deephaven.db.v2.select.python.FormulaKernelPythonSingularFunction
 */
class FormulaKernelPythonChunkedFunction extends FormulaKernelTypedBase implements FormulaKernel {

    private static final String CALL_METHOD = "__call__";

    // this is a python function whose arguments can accept arrays
    private final PyObject function;

    FormulaKernelPythonChunkedFunction(PyObject function) {
        this.function = Objects.requireNonNull(function, "function");
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableByteChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final byte[] output = function
                .call(byte[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableBooleanChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final boolean[] output = function
                .call(boolean[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableCharChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final char[] output = function
                .call(char[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableShortChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final short[] output = function
                .call(short[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableIntChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final int[] output = function
                .call(int[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableLongChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final long[] output = function
                .call(long[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableFloatChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final float[] output = function
                .call(float[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public void applyFormulaChunk(
            FillContext __context,
            WritableDoubleChunk<? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);
        final double[] output = function
                .call(double[].class, CALL_METHOD, args.getParamTypes(), args.getParams());
        __destination.copyFromTypedArray(output, 0, 0, output.length);
    }

    @Override
    public <T> void applyFormulaChunk(
            FillContext __context,
            WritableObjectChunk<T, ? super Values> __destination,
            Chunk<? extends Values>[] __sources) {
        final io.deephaven.db.v2.select.python.ArgumentsChunked args =
                io.deephaven.db.v2.select.python.ArgumentsChunked.buildArguments(__sources);

        // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to return
        // the array type
        final Object[] output = function
                .call(Object[].class, CALL_METHOD, args.getParamTypes(), args.getParams());

        // noinspection unchecked
        __destination.copyFromTypedArray((T[]) output, 0, 0, output.length);
    }

    @Override
    public FillContext makeFillContext(int __chunkCapacity) {
        return FillContextPython.EMPTY;
    }
}
