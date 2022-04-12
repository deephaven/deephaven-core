package io.deephaven.engine.table.impl.select.python;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.lang.QueryScopeParam;
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

    private boolean initialized;

    private FormulaColumnPython(String columnName,
            io.deephaven.engine.table.impl.select.python.DeephavenCompatibleFunction dcf) {
        super(columnName, "<python-formula>", true);
        this.dcf = Objects.requireNonNull(dcf);
    }

    private void initFromDef(Map<String, ColumnDefinition<?>> columnNameMap) {
        if (initialized) {
            throw new IllegalStateException("Already initialized");
        }
        returnedType = dcf.getReturnedType();
        this.initialized = true;
    }

    @Override
    protected final FormulaKernelFactory getFormulaKernelFactory() {
        return this;
    }

    @Override
    public final List<String> initDef(Map<String, ColumnDefinition<?>> columnNameMap) {
        if (!initialized) {
            initFromDef(columnNameMap);
            applyUsedVariables(columnNameMap, new LinkedHashSet<>(dcf.getColumnNames()));
        }
        return usedColumns;
    }

    @Override
    public boolean isStateless() {
        // we can't control python
        return false;
    }

    @Override
    public boolean preventsParallelization() {
        return true;
    }

    @Override
    protected final FormulaSourceDescriptor getSourceDescriptor() {
        if (!initialized) {
            throw new IllegalStateException("Must be initialized first");
        }
        return new FormulaSourceDescriptor(
                returnedType,
                dcf.getColumnNames().toArray(new String[0]),
                ZERO_LENGTH_STRING_ARRAY,
                ZERO_LENGTH_STRING_ARRAY);
    }

    @Override
    public final SelectColumn copy() {
        return new FormulaColumnPython(columnName, dcf);
    }

    @Override
    public final FormulaKernel createInstance(Vector<?>[] arrays, QueryScopeParam<?>[] params) {
        if (!initialized) {
            throw new IllegalStateException("Must be initialized first");
        }
        return dcf.toFormulaKernel();
    }
}
