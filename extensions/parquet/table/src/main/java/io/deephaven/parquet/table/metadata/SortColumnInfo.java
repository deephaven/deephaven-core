//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

@Immutable
@SimpleStyle
@JsonSerialize(as = ImmutableSortColumnInfo.class)
@JsonDeserialize(as = ImmutableSortColumnInfo.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class SortColumnInfo {

    public enum SortDirection {
        Ascending, Descending
    }

    @Parameter
    public abstract String columnName();

    @Parameter
    public abstract SortDirection sortDirection();

    @Check
    final void checkColumnName() {
        ColumnName.of(columnName());
    }

    public final SortColumn sortColumn() {
        return sortDirection() == SortDirection.Ascending
                ? SortColumn.asc(ColumnName.of(columnName()))
                : SortColumn.desc(ColumnName.of(columnName()));
    }

    public static List<SortColumn> sortColumns(@NotNull final List<SortColumnInfo> sortColumnInfos) {
        return sortColumnInfos.stream().map(SortColumnInfo::sortColumn).collect(Collectors.toList());
    }

    public static SortColumnInfo of(@NotNull final SortColumn sortColumn) {
        return of(sortColumn.column().name(), sortColumn.order() == SortColumn.Order.ASCENDING
                ? SortDirection.Ascending
                : SortDirection.Descending);
    }

    public static SortColumnInfo of(@NotNull final String columnName, @NotNull final SortDirection sortDirection) {
        return ImmutableSortColumnInfo.of(columnName, sortDirection);
    }
}
