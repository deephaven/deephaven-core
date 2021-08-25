package io.deephaven.db.v2.select.python;

import io.deephaven.db.v2.select.Formula.FillContext;
import io.deephaven.db.v2.select.FormulaKernelTypedBase;
import io.deephaven.db.v2.select.formula.FormulaKernel;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import org.jpy.PyObject;

import java.util.Objects;

/**
 * A python formula kernel which is implemented by iterating over the input chunks and calling the
 * python function N times.
 *
 * @see FormulaKernelPythonChunkedFunction
 */
class FormulaKernelPythonSingularFunction extends FormulaKernelTypedBase implements FormulaKernel {

    private static final String CALL_METHOD = "__call__";

    private final PyObject function;

    FormulaKernelPythonSingularFunction(PyObject function) {
        this.function = Objects.requireNonNull(function, "function");
    }


    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableByteChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final byte output = function
                .call(byte.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableBooleanChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final boolean output = function
                .call(boolean.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableCharChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final char output = function
                .call(char.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableShortChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final short output = function
                .call(short.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableIntChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final int output = function
                .call(int.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableLongChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final long output = function
                .call(long.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableFloatChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final float output = function
                .call(float.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public void applyFormulaChunk(
        FillContext __context,
        WritableDoubleChunk<? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            final double output = function
                .call(double.class, CALL_METHOD, types,
                    ArgumentsSingular.buildArguments(__sources, i));
            __destination.set(i, output);
        }
    }

    @Override
    public <T> void applyFormulaChunk(
        FillContext __context,
        WritableObjectChunk<T, ? super Values> __destination,
        Chunk<? extends Values>[] __sources) {
        final Class<?>[] types = ArgumentsSingular.buildParamTypes(__sources);
        final int L = __destination.size();
        for (int i = 0; i < L; i++) {
            // this is LESS THAN IDEAL - it would be much better if ObjectChunk would be able to
            // return
            // the non-array type
            final Object output = function
                .call(Object.class, CALL_METHOD, types, ArgumentsSingular
                    .buildArguments(__sources, i));

            // noinspection unchecked
            __destination.set(i, (T) output);
        }
    }

    @Override
    public FillContext makeFillContext(int __chunkCapacity) {
        return FillContextPython.EMPTY;
    }
}
