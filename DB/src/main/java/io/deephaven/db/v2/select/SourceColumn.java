/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.NoSuchColumnException;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.SparseArrayColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SourceColumn implements SelectColumn {

    @NotNull private final String sourceName;
    @NotNull private final String destName;
    private ColumnDefinition sourceDefinition;
    private ColumnSource sourceColumn;

    public SourceColumn(String columnName) {
        this(columnName,columnName);
    }

    public SourceColumn(String sourceName,String destName) {
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.destName = NameValidator.validateColumnName(destName);
    }

    @Override
    public List<String> initInputs(Table table) {
        this.sourceColumn = table.getColumnSource(sourceName);
        if (sourceColumn == null) {
            throw new NoSuchColumnException(table.getDefinition().getColumnNames(), sourceName);
        }
        return Collections.singletonList(sourceName);
    }

    @Override
    public List<String> initInputs(Index index, Map<String, ? extends ColumnSource> columnsOfInterest) {
        this.sourceColumn = columnsOfInterest.get(sourceName);
        if (sourceColumn == null) {
            throw new NoSuchColumnException(columnsOfInterest.keySet(), sourceName);
        }
        return Collections.singletonList(sourceName);
    }

    @Override
    public List<String> initDef(Map<String, ColumnDefinition> columnDefinitionMap) {
        sourceDefinition = columnDefinitionMap.get(sourceName);
        if (sourceDefinition == null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), sourceName);
        }
        return Collections.singletonList(sourceName);
    }

    @Override
    public Class getReturnedType() {
        // Try to be a little flexible, depending on whether initInputs or initDef was called.
        if (sourceDefinition != null) {
            return sourceDefinition.getDataType();
        }
        return sourceColumn.getType();
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
    public ColumnSource getDataView() {
        if (sourceColumn == null) {
            throw Assert.statementNeverExecuted("sourceColumn is null for " + toString() + ": ");
        }
        return sourceColumn;
    }

    @NotNull
    @Override
    public ColumnSource getLazyView() {
        return sourceColumn;
    }

    @Override
    public String getName() {
        return destName;
    }

    @NotNull
    public String getSourceName() { return sourceName; }

    @Override
    public MatchPair getMatchPair() {
        return new MatchPair(destName, sourceName);
    }

    @Override
    public WritableSource newDestInstance(long size) {
        Class type = sourceColumn.getType();
        if (DbArrayBase.class.isAssignableFrom(type)) {
            return SparseArrayColumnSource.getSparseMemoryColumnSource(size, type, sourceColumn.getComponentType());
        } else {
            return SparseArrayColumnSource.getSparseMemoryColumnSource(size, type);
        }
    }

    @Override
    public boolean isRetain() {
        return destName.equals(sourceName);
    }

    @Override
    public String toString() {
        return destName + "=" + sourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SourceColumn that = (SourceColumn) o;

        return sourceName.equals(that.sourceName) && destName.equals(that.destName);
    }

    @Override
    public int hashCode() {
        int result = sourceName.hashCode();
        result = 31 * result + destName.hashCode();
        return result;
    }


    @Override
    public boolean disallowRefresh() {
        return false;
    }

    @Override
    public SourceColumn copy() {
        return new SourceColumn(sourceName, destName);
    }
}
