package io.deephaven.db.v2.select;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A SelectColumn implementation that can be used to replace columns with
 * {@link NullValueColumnSource}s
 */
public class NullSelectColumn<T> implements SelectColumn {
    private final String name;
    private final NullValueColumnSource<T> nvcs;

    public NullSelectColumn(final Class<T> type, final Class<T> elementType, final String name) {
        nvcs = NullValueColumnSource.getInstance(type, elementType);
        this.name = name;
    }

    @Override
    public List<String> initInputs(final Table table) {
        return Collections.emptyList();
    }

    @Override
    public List<String> initInputs(final Index index,
        final Map<String, ? extends ColumnSource> columnsOfInterest) {
        return Collections.emptyList();
    }

    @Override
    public List<String> initDef(final Map<String, ColumnDefinition> columnDefinitionMap) {
        return Collections.emptyList();
    }

    @Override
    public Class getReturnedType() {
        return nvcs.getType();
    }

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public ColumnSource getDataView() {
        return nvcs;
    }

    @NotNull
    @Override
    public ColumnSource getLazyView() {
        return nvcs;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MatchPair getMatchPair() {
        return new MatchPair(name, name);
    }

    @Override
    public WritableSource newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, nvcs.getType());
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean disallowRefresh() {
        return false;
    }

    @Override
    public SelectColumn copy() {
        // noinspection unchecked
        return new NullSelectColumn<>(getReturnedType(), nvcs.getComponentType(), name);
    }
}
