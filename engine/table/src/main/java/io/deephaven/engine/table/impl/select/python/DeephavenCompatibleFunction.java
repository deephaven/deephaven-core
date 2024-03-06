//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel;
import io.deephaven.engine.table.impl.select.ConditionFilter.FilterKernel.Context;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import org.jpy.PyObject;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A Deephaven-compatible functions holds a native python function with associated typing information, used to help
 * implement {@link io.deephaven.engine.table.impl.select.AbstractConditionFilter} and {@link FormulaColumnPython}.
 */
public class DeephavenCompatibleFunction {

    public static DeephavenCompatibleFunction create(
            PyObject function,

            // todo: python can't convert from java type to Class<?> (ie, java_func_on_type(jpy.get_type('...')))
            // but it *will* match on object, and unwrap the actual java type...
            Object returnedType,

            // todo: python can't convert from list of strings to List<String>
            // but it can convert from list of strings to String[]...
            String[] columnNames,
            ArgumentsChunked argumentsChunked,
            boolean isVectorized) {
        return new DeephavenCompatibleFunction(function, (Class) returnedType, Arrays.asList(columnNames),
                argumentsChunked, isVectorized);
    }

    private final PyObject function;
    private final Class<?> returnedType; // the un-vectorized type (if this function is vectorized)
    private final List<String> columnNames;

    private final ArgumentsChunked argumentsChunked;
    private final boolean isVectorized;

    private DeephavenCompatibleFunction(
            PyObject function,
            Class<?> returnedType,
            List<String> columnNames,
            ArgumentsChunked argumentsChunked,
            boolean isVectorized) {
        this.function = Objects.requireNonNull(function, "function");
        this.returnedType = Objects.requireNonNull(returnedType, "returnedType");
        this.columnNames = Objects.requireNonNull(columnNames, "columnNames");
        this.argumentsChunked = argumentsChunked;
        this.isVectorized = isVectorized;
    }

    public FormulaKernel toFormulaKernel() {
        return isVectorized ? new FormulaKernelPythonChunkedFunction(function, argumentsChunked)
                : new FormulaKernelPythonSingularFunction(function);
    }

    public FilterKernel<Context> toFilterKernel() {
        if (returnedType != boolean.class && !Boolean.class.equals(returnedType)) {
            throw new IllegalStateException("FilterKernel functions must be annotated with a boolean return type");
        }
        return isVectorized ? new FilterKernelPythonChunkedFunction(function, argumentsChunked)
                : new FilterKernelPythonSingularFunction(function);
    }

    public PyObject getFunction() {
        return function;
    }

    public Class<?> getReturnedType() {
        return returnedType;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean isVectorized() {
        return isVectorized;
    }
}
