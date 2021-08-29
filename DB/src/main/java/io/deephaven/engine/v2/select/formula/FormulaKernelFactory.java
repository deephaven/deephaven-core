package io.deephaven.engine.v2.select.formula;

import io.deephaven.engine.tables.dbarrays.DbArrayBase;
import io.deephaven.engine.tables.select.Param;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(DbArrayBase<?>[] arrays, Param<?>[] params);
}
