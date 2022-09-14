/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.select.AbstractFormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.engine.table.impl.select.formula.FormulaKernelFactory;
import io.deephaven.engine.table.impl.select.formula.FormulaSourceDescriptor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

/**
 * A formula column for python native code.
 */
public class FormulaColumnPython extends AbstractFormulaColumn implements FormulaKernelFactory {

    @SuppressWarnings("unused") // called from python
    public static FormulaColumnPython create(String columnName,
            io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction dcf) {
        return new FormulaColumnPython(columnName, dcf);
    }

    private final io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction dcf;

    private FormulaColumnPython(String columnName,
            io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction dcf) {
        super(columnName, "<python-formula>");
        this.dcf = Objects.requireNonNull(dcf);
    }

    @Override
    public final List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        if (formulaFactory != null) {
            validateColumnDefinition(columnDefinitionMap);
        } else {
            returnedType = dcf.getReturnedType();
            applyUsedVariables(columnDefinitionMap, new LinkedHashSet<>(dcf.getColumnNames()));
            formulaFactory = createKernelFormulaFactory(this);
        }

        return usedColumns;
    }

    @Override
    public boolean isStateless() {
        // we can't control python
        return false;
    }

    @Override
    protected final FormulaSourceDescriptor getSourceDescriptor() {
        return new FormulaSourceDescriptor(
                returnedType,
                dcf.getColumnNames().toArray(new String[0]),
                ZERO_LENGTH_STRING_ARRAY,
                ZERO_LENGTH_STRING_ARRAY);
    }

    @Override
    public final SelectColumn copy() {
        final FormulaColumnPython copy = new FormulaColumnPython(columnName, dcf);
        if (formulaFactory != null) {
            // copy all initDef state
            copy.returnedType = returnedType;
            onCopy(copy);
        }
        return copy;
    }

    @Override
    public final FormulaKernel createInstance(Vector<?>[] arrays, QueryScopeParam<?>[] params) {
        if (formulaFactory == null) {
            throw new IllegalStateException("Must be initialized first");
        }
        return dcf.toFormulaKernel();
    }
}
