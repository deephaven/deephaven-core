package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.table.lang.QueryScopeParam;
import io.deephaven.vector.Vector;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(Vector<?>[] arrays, QueryScopeParam<?>[] params);
}
