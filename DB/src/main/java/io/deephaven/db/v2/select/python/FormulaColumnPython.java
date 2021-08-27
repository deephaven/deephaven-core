package io.deephaven.db.v2.select.python;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.select.Param;
import io.deephaven.db.v2.select.AbstractFormulaColumn;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.formula.FormulaKernel;
import io.deephaven.db.v2.select.formula.FormulaKernelFactory;
import io.deephaven.db.v2.select.formula.FormulaSourceDescriptor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.deephaven.datastructures.util.CollectionUtil;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

/**
 * A formula column for python native code.
 */
public class FormulaColumnPython extends AbstractFormulaColumn implements FormulaKernelFactory {

    @SuppressWarnings("unused") // called from python
    public static FormulaColumnPython create(String columnName,
            io.deephaven.db.v2.select.python.DeephavenCompatibleFunction dcf) {
        return new FormulaColumnPython(columnName, dcf);
    }

    private final io.deephaven.db.v2.select.python.DeephavenCompatibleFunction dcf;

    private boolean initialized;

    private FormulaColumnPython(String columnName, io.deephaven.db.v2.select.python.DeephavenCompatibleFunction dcf) {
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
    public final FormulaKernel createInstance(DbArrayBase<?>[] arrays, Param<?>[] params) {
        if (!initialized) {
            throw new IllegalStateException("Must be initialized first");
        }
        return dcf.toFormulaKernel();
    }
}
