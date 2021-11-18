package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.vector.Vector;
import io.deephaven.engine.tables.select.Param;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(Vector<?>[] arrays, Param<?>[] params);
}
