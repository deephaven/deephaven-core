package io.deephaven.engine.v2.select.formula;

import io.deephaven.engine.structures.vector.DbArrayBase;
import io.deephaven.engine.tables.select.Param;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(DbArrayBase<?>[] arrays, Param<?>[] params);
}
