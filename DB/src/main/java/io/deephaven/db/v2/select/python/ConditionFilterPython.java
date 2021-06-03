package io.deephaven.db.v2.select.python;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.lang.DBLanguageParser;
import io.deephaven.db.tables.utils.DBTimeUtils.Result;
import io.deephaven.db.v2.select.AbstractConditionFilter;
import io.deephaven.db.v2.select.ConditionFilter;
import io.deephaven.db.v2.select.ConditionFilter.ChunkFilter;
import io.deephaven.db.v2.utils.Index;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A condition filter for python native code.
 */
public class ConditionFilterPython extends AbstractConditionFilter {

    private Filter filter;

    @SuppressWarnings("unused") // called from python
    public static ConditionFilterPython create(DeephavenCompatibleFunction dcf) {
        return new ConditionFilterPython(dcf);
    }

    private final DeephavenCompatibleFunction dcf;

    private ConditionFilterPython(DeephavenCompatibleFunction dcf) {
        this(Collections.emptyMap(), dcf);
    }

    private ConditionFilterPython(Map<String, String> renames, DeephavenCompatibleFunction dcf) {
        super("<python-condition>", renames, false);
        this.dcf = Objects.requireNonNull(dcf);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        // no-op
    }

    @Override
    protected Filter getFilter(Table table, Index fullSet) {
        return new ChunkFilter(
            dcf.toFilterKernel(),
            dcf.getColumnNames().toArray(new String[0]),
            ConditionFilter.CHUNK_SIZE);
    }

    @Override
    protected void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public AbstractConditionFilter copy() {
        return new ConditionFilterPython(dcf);
    }

    @Override
    public AbstractConditionFilter renameFilter(Map<String, String> renames) {
        return new ConditionFilterPython(renames, dcf);
    }

    @Override
    public boolean isRefreshing() {
        return false;
    }

    @Override
    public boolean canMemoize() {
        return false;
    }

    @Override
    protected void generateFilterCode(TableDefinition tableDefinition, Result timeConversionResult, DBLanguageParser.Result result) {
        throw new UnsupportedOperationException("ConditionFilterPython does not generate java code");
    }
}
