//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base {@link SelectColumn} implementation to wrap transformer functions for {@link PartitionedTable#transform} and
 * {@link PartitionedTable#partitionedTransform}.
 */
abstract class BaseTableTransformationColumn implements SelectColumn {

    BaseTableTransformationColumn() {}

    @Override
    public final Class<?> getReturnedType() {
        return Table.class;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return null;
    }

    @Override
    public final List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public final ColumnSource<?> getLazyView() {
        return getDataView();
    }

    @Override
    public final MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final WritableColumnSource<?> newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, Table.class);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, Table.class, null);
    }

    @Override
    public final boolean isRetain() {
        return false;
    }

    @Override
    public final boolean isStateless() {
        return true;
    }

    static ColumnSource<Table> getAndValidateInputColumnSource(
            @NotNull final String inputColumnName,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        final ColumnSource<?> inputColumnSource = columnsOfInterest.get(inputColumnName);
        if (inputColumnSource == null) {
            throw new NoSuchColumnException(columnsOfInterest.keySet(), inputColumnName);
        }
        if (!Table.class.isAssignableFrom(inputColumnSource.getType())) {
            throw new IllegalArgumentException("Input column \"" + inputColumnName
                    + "\" has unexpected type " + inputColumnSource.getType());
        }
        // noinspection unchecked
        return (ColumnSource<Table>) inputColumnSource;
    }

    static void validateInputColumnDefinition(
            @NotNull final String inputColumnName,
            @NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        final ColumnDefinition<?> inputColumnDefinition = columnDefinitionMap.get(inputColumnName);
        if (inputColumnDefinition == null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), inputColumnName);
        }
        if (!Table.class.isAssignableFrom(inputColumnDefinition.getDataType())) {
            throw new IllegalArgumentException("Input column \"" + inputColumnName
                    + "\" has unexpected type " + inputColumnDefinition.getDataType());
        }
    }
}
