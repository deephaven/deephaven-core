package io.deephaven.engine.v2.select;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.structures.rowset.Index;

import java.util.Collections;
import java.util.List;

/**
 * A Select filter that always returns an empty index.
 */
public class SelectNoneFilter extends SelectFilterImpl {

    public static final SelectNoneFilter INSTANCE = new SelectNoneFilter();

    private SelectNoneFilter() {}

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {}

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        return Index.FACTORY.getEmptyIndex();
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {}

    @Override
    public SelectFilter copy() {
        return INSTANCE;
    }
}
