package io.deephaven.engine.v2.select.formula;

import io.deephaven.engine.vector.Vector;
import io.deephaven.engine.tables.select.Param;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(Vector<?>[] arrays, Param<?>[] params);
}
