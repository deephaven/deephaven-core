package io.deephaven.db.v2.parquet.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value;

/**
 * Representation class for grouping column information stored in key-value metadata for
 * Deephaven-written Parquet files.
 */
@Value.Immutable
@SimpleStyle
@JsonSerialize(as = ImmutableGroupingColumnInfo.class)
@JsonDeserialize(as = ImmutableGroupingColumnInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class GroupingColumnInfo {

    /**
     * @return The column name
     */
    @Value.Parameter
    public abstract String columnName();

    /**
     * @return The relative path name for the column's grouping sidecar table
     */
    @Value.Parameter
    public abstract String groupingTablePath();

    @Value.Check
    final void checkColumnName() {
        if (columnName().isEmpty()) {
            throw new IllegalArgumentException("Empty column name");
        }
    }

    @Value.Check
    final void checkGroupingTablePath() {
        if (groupingTablePath().isEmpty()) {
            throw new IllegalArgumentException("Empty grouping table path");
        }
    }

    public static GroupingColumnInfo of(String columnName, String groupingTablePath) {
        return ImmutableGroupingColumnInfo.of(columnName, groupingTablePath);
    }
}
