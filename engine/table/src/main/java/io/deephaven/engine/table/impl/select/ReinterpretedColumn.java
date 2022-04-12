/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Allows us to support ColumnSource reinterpretation via view-type Table operations. Currently, this is only exposed in
 * V2 tables' support for dateTimeColumnAsNanos().
 *
 * TODO: If we come up with other valid, useful reinterpretations, it would be trivial to create a general purpose
 * syntax for use in view()/updateView() column expressions.
 *
 * The syntax I have in mind is: "&lt;ColumnNameB&gt;=&lt;ColumnNameA&gt;.as(&lt;ClassName&gt;)"
 * "&lt;ColumnName&gt;.as(&lt;ClassName&gt;)"
 *
 * Making this work would consist of any one of: 1. Adding a V1 version and updating SelectColumnFactory and
 * SelectColumnAdaptor 2. Adding the appropriate if-regex-matches to realColumn selection in V2 SwitchColumn 3. Creating
 * a V2-native SelectColumnFactory
 */
public class ReinterpretedColumn<S, D> implements SelectColumn {

    @NotNull
    private final String sourceName;
    @NotNull
    private final Class<S> sourceDataType;
    @NotNull
    private final String destName;
    @NotNull
    private final Class<D> destDataType;

    private ColumnSource<S> sourceColumnSource;

    public ReinterpretedColumn(String sourceName, Class<S> sourceDataType, String destName, Class<D> destDataType) {
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.sourceDataType = Require.neqNull(sourceDataType, "sourceDataType");
        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        Require.gtZero(destName.length(), "destName.length()");
    }

    @Override
    public String toString() {
        return "reinterpretAs(" + sourceName + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(Table table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        // noinspection unchecked
        final ColumnSource<S> localSourceColumnSource = (ColumnSource<S>) columnsOfInterest.get(sourceName);
        if (localSourceColumnSource == null) {
            throw new NoSuchColumnException(columnsOfInterest.keySet(), sourceName);
        }
        if (!localSourceColumnSource.getType().equals(sourceDataType)) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + localSourceColumnSource.getType() + ", expected " + sourceDataType);
        }
        if (!(localSourceColumnSource.allowsReinterpret(destDataType))) {
            throw new IllegalArgumentException("Source column " + sourceName + " (Class="
                    + localSourceColumnSource.getClass() + ") - cannot be reinterpreted as " + destDataType);
        }
        // noinspection unchecked
        sourceColumnSource = (ColumnSource<S>) columnsOfInterest.get(sourceName);
        return getColumns();
    }

    @Override
    public List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        // noinspection unchecked
        final ColumnDefinition<S> sourceColumnDefinition = (ColumnDefinition<S>) columnDefinitionMap.get(sourceName);
        if (sourceColumnDefinition == null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), sourceName);
        }
        if (!sourceColumnDefinition.getDataType().equals(sourceDataType)) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + sourceColumnDefinition.getDataType() + ", expected " + sourceDataType);
        }
        return getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return destDataType;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(sourceName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public ColumnSource<D> getDataView() {
        final ColumnSource<D> result = sourceColumnSource.reinterpret(destDataType);
        if (!result.getType().equals(destDataType)) {
            throw new IllegalArgumentException("Reinterpreted column from " + sourceName + " has wrong data type "
                    + result.getType() + ", expected " + destDataType);
        }
        return result;
    }

    @NotNull
    @Override
    public ColumnSource<D> getLazyView() {
        return getDataView();
    }

    @Override
    public String getName() {
        return destName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableColumnSource<?> newDestInstance(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ReinterpretedColumn<?, ?> that = (ReinterpretedColumn<?, ?>) o;

        return sourceName.equals(that.sourceName) && sourceDataType.equals(that.sourceDataType)
                && destName.equals(that.destName) && destDataType.equals(that.destDataType);
    }

    @Override
    public int hashCode() {
        int result = sourceName.hashCode();
        result = 31 * result + sourceDataType.hashCode();
        result = 31 * result + destName.hashCode();
        result = 31 * result + destDataType.hashCode();
        return result;
    }

    @Override
    public boolean disallowRefresh() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return sourceColumnSource.isStateless();
    }

    @Override
    public ReinterpretedColumn<S, D> copy() {
        return new ReinterpretedColumn<>(sourceName, sourceDataType, destName, destDataType);
    }
}
