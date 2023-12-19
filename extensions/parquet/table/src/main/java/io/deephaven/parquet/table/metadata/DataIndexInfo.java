package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.util.string.StringUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Representation class for data index information stored in key-value metadata for Deephaven-written Parquet files.
 */
@Value.Immutable
@SimpleStyle
@JsonSerialize(as = ImmutableDataIndexInfo.class)
@JsonDeserialize(as = ImmutableDataIndexInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class DataIndexInfo {

    /**
     * @return The column names
     */
    @Value.Parameter
    public abstract List<String> columns();

    /**
     * @return The relative path name for the columns' data index sidecar table
     */
    @Value.Parameter
    public abstract String indexTablePath();

    @Value.Check
    final void checkColumns() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("No columns provided");
        }
        if (columns().stream().anyMatch(StringUtils::isNullOrEmpty)) {
            throw new IllegalArgumentException("Empty column name");
        }
    }

    @Value.Check
    final void checkIndexTablePath() {
        if (indexTablePath().isEmpty()) {
            throw new IllegalArgumentException("Empty index table path");
        }
    }

    public boolean matchesColumns(final String... columnsToMatch) {
        final List<String> columnsToMatchList = new ArrayList<>(columnsToMatch.length);
        Collections.addAll(columnsToMatchList, columnsToMatch);
        columnsToMatchList.sort(String::compareTo);
        return columnsToMatchList.equals(columns());
    }

    public static DataIndexInfo of(@NotNull final String indexTablePath, final String... columnNames) {
        final List<String> columnNamesList = new ArrayList<>(columnNames.length);
        Collections.addAll(columnNamesList, columnNames);
        columnNamesList.sort(String::compareTo);
        return ImmutableDataIndexInfo.of(columnNamesList, indexTablePath);
    }
}
