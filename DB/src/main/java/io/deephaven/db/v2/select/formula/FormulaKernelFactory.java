package io.deephaven.db.v2.select.formula;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.select.Param;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(DbArrayBase<?>[] arrays, Param<?>[] params);
}
