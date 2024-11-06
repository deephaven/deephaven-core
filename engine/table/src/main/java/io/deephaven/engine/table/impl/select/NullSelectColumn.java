//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A SelectColumn implementation that can be used to replace columns with {@link NullValueColumnSource}s
 */
public class NullSelectColumn<T> implements SelectColumn {
    private final String name;
    private final NullValueColumnSource<T> nvcs;

    public NullSelectColumn(final Class<T> type, final Class<?> elementType, final String name) {
        nvcs = NullValueColumnSource.getInstance(type, elementType);
        this.name = name;
    }

    @Override
    public List<String> initInputs(final TrackingRowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        return Collections.emptyList();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return Collections.emptyList();
    }

    @Override
    public Class<?> getReturnedType() {
        return nvcs.getType();
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return nvcs.getComponentType();
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
    public ColumnSource<?> getDataView() {
        return nvcs;
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
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
    public WritableColumnSource<?> newDestInstance(final long size) {
        return nvcs;
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(final long size) {
        return nvcs;
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return true;
    }

    @Override
    public SelectColumn copy() {
        // noinspection unchecked,rawtypes
        return new NullSelectColumn(getReturnedType(), nvcs.getComponentType(), name);
    }
}
