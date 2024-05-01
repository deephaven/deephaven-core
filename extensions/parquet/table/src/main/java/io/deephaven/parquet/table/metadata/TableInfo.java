//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Representation class for per-table information stored in key-value metadata for Deephaven-written Parquet files.
 */
@Immutable
@BuildableStyle
@JsonSerialize(as = ImmutableTableInfo.class)
@JsonDeserialize(as = ImmutableTableInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class TableInfo {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        OBJECT_MAPPER = objectMapper;
    }

    public final String serializeToJSON() throws IOException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    public static TableInfo deserializeFromJSON(@NotNull final String tableInfoRaw) throws IOException {
        return OBJECT_MAPPER.readValue(tableInfoRaw, ImmutableTableInfo.class);
    }

    public final Map<String, GroupingColumnInfo> groupingColumnMap() {
        return groupingColumns().stream()
                .collect(Collectors.toMap(GroupingColumnInfo::columnName, Function.identity()));
    }

    public final Map<String, ColumnTypeInfo> columnTypeMap() {
        return columnTypes().stream().collect(Collectors.toMap(ColumnTypeInfo::columnName, Function.identity()));
    }

    /**
     * @return The Deephaven release version used to write the parquet file
     */
    @Default
    public String version() {
        final String version = TableInfo.class.getPackage().getImplementationVersion();
        // noinspection ReplaceNullCheck
        if (version == null) {
            // When the code is run from class files as opposed to jars, like in unit tests
            return "unknown";
        }
        return version;
    }

    /**
     * @return List of {@link GroupingColumnInfo grouping columns} for columns with grouped data
     */
    public abstract List<GroupingColumnInfo> groupingColumns();

    /**
     * @return List of {@link DataIndexInfo data indexes} for this table
     */
    public abstract List<DataIndexInfo> dataIndexes();

    /**
     * @return List of {@link ColumnTypeInfo column types} for columns requiring non-default deserialization or type
     *         selection
     */
    public abstract List<ColumnTypeInfo> columnTypes();

    /**
     * @return List of {@link SortColumnInfo sort columns} representing the sort order of the table. Note that these are
     *         ordered by precedence, representing a multi-column sort.
     */
    public abstract List<SortColumnInfo> sortingColumns();

    @Check
    final void checkVersion() {
        if (version().isEmpty()) {
            throw new IllegalArgumentException("Empty version");
        }
    }

    public static TableInfo.Builder builder() {
        return ImmutableTableInfo.builder();
    }

    public interface Builder {

        Builder version(String version);

        Builder addGroupingColumns(GroupingColumnInfo groupingColumn);

        Builder addGroupingColumns(GroupingColumnInfo... groupingColumns);

        Builder addAllGroupingColumns(Iterable<? extends GroupingColumnInfo> groupingColumns);

        Builder addDataIndexes(DataIndexInfo dataIndex);

        Builder addDataIndexes(DataIndexInfo... dataIndexes);

        Builder addAllDataIndexes(Iterable<? extends DataIndexInfo> dataIndexes);

        Builder addColumnTypes(ColumnTypeInfo columnType);

        Builder addColumnTypes(ColumnTypeInfo... columnTypes);

        Builder addAllColumnTypes(Iterable<? extends ColumnTypeInfo> columnTypes);

        Builder addSortingColumns(SortColumnInfo sortColumns);

        Builder addSortingColumns(SortColumnInfo... sortColumns);

        Builder addAllSortingColumns(Iterable<? extends SortColumnInfo> sortColumns);

        TableInfo build();
    }
}
