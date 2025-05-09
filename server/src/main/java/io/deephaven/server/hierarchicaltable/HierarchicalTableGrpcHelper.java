//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.hierarchicaltable;

import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.AbsoluteSortColumnConventions;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.AbsoluteSortColumnConventions.baseColumnNameToAbsoluteName;

/**
 * Utility functions for the HierarchicalTableServiceGrpcImpl that are useful for other components.
 */
@InternalUseOnly
public class HierarchicalTableGrpcHelper {
    /**
     * For each {@link SortDescriptor} produce a {@link SortColumn}, verifying against the list of sortable columns.
     *
     * @param sortsList the list of sorts to translate
     * @param sortableColumnsSupplier a supplier of sortable column names
     * @return a list of translated sorts, validated against the sortable columns
     */
    @Nullable
    public static Collection<SortColumn> translateAndValidateSorts(
            @NotNull final List<SortDescriptor> sortsList,
            @NotNull final Supplier<Set<String>> sortableColumnsSupplier) {
        if (sortsList.isEmpty()) {
            return null;
        }
        final Collection<SortColumn> translatedSorts = sortsList.stream()
                .map(HierarchicalTableGrpcHelper::translateSort)
                .collect(Collectors.toList());
        final Set<String> sortableColumnNames = sortableColumnsSupplier.get();
        if (sortableColumnNames != null) {
            if (sortableColumnNames.isEmpty()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Sorting is not supported on this hierarchical table");
            }
            final Collection<String> unavailableSortColumnNames = translatedSorts.stream()
                    .map(sc -> AbsoluteSortColumnConventions.stripAbsoluteColumnName(sc.column().name()))
                    .filter(scn -> !sortableColumnNames.contains(scn))
                    .collect(Collectors.toList());
            if (!unavailableSortColumnNames.isEmpty()) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Sorting attempted on restricted column(s): "
                                + unavailableSortColumnNames.stream().collect(Collectors.joining(", ", "[", "]"))
                                + ", available column(s) for sorting are: "
                                + sortableColumnNames.stream().collect(Collectors.joining(", ", "[", "]")));
            }
        }
        return translatedSorts;
    }

    private static SortColumn translateSort(@NotNull final SortDescriptor sortDescriptor) {
        switch (sortDescriptor.getDirection()) {
            case DESCENDING:
                return SortColumn.desc(ColumnName.of(sortDescriptor.getIsAbsolute()
                        ? baseColumnNameToAbsoluteName(sortDescriptor.getColumnName())
                        : sortDescriptor.getColumnName()));
            case ASCENDING:
                return SortColumn.asc(ColumnName.of(sortDescriptor.getIsAbsolute()
                        ? baseColumnNameToAbsoluteName(sortDescriptor.getColumnName())
                        : sortDescriptor.getColumnName()));
            case UNKNOWN:
            case REVERSE:
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Unsupported or unknown sort direction: " + sortDescriptor.getDirection());
        }
    }
}
