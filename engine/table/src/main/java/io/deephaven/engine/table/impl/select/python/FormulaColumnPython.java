//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.context.QueryScopeParam;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.select.AbstractFormulaColumn;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.formula.FormulaKernel;
import io.deephaven.engine.table.impl.select.formula.FormulaKernelFactory;
import io.deephaven.engine.table.impl.select.formula.FormulaSourceDescriptor;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A formula column for python native code.
 */
public class FormulaColumnPython extends AbstractFormulaColumn implements FormulaKernelFactory {

    @SuppressWarnings("unused") // called from python
    public static FormulaColumnPython create(String columnName, DeephavenCompatibleFunction dcf) {
        return new FormulaColumnPython(columnName, dcf);
    }

    private final DeephavenCompatibleFunction dcf;

    private FormulaColumnPython(String columnName,
            DeephavenCompatibleFunction dcf) {
        super(columnName, "<python-formula>");
        this.dcf = Objects.requireNonNull(dcf);
    }

    @Override
    public final List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        if (formulaFactoryFuture != null) {
            validateColumnDefinition(columnDefinitionMap);
        } else {
            returnedType = dcf.getReturnedType();
            applyUsedVariables(columnDefinitionMap, new LinkedHashSet<>(dcf.getColumnNames()), Map.of());
            formulaFactoryFuture = createKernelFormulaFactory(CompletionStageFuture.completedFuture(this));
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
                dcf.getColumnNames().toArray(String[]::new),
                ArrayTypeUtils.EMPTY_STRING_ARRAY,
                ArrayTypeUtils.EMPTY_STRING_ARRAY);
    }

    @Override
    public final SelectColumn copy() {
        final FormulaColumnPython copy = new FormulaColumnPython(columnName, dcf);
        if (formulaFactoryFuture != null) {
            // copy all initDef state
            copy.returnedType = returnedType;
            onCopy(copy);
        }
        return copy;
    }

    @Override
    public final FormulaKernel createInstance(Vector<?>[] arrays, QueryScopeParam<?>[] params) {
        if (formulaFactoryFuture == null) {
            throw new IllegalStateException("Must be initialized first");
        }
        return dcf.toFormulaKernel();
    }
}
