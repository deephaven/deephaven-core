//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.vector.Vector;

public interface FormulaKernelFactory {
    FormulaKernel createInstance(Vector<?>[] arrays, QueryScopeParam<?>[] params);
}
