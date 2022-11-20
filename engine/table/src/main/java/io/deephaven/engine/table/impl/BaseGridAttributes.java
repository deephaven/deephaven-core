package io.deephaven.engine.table.impl;

import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default implementation for {@link GridAttributes}.
 */
public abstract class BaseGridAttributes<IFACE_TYPE extends GridAttributes<IFACE_TYPE>, IMPL_TYPE extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>>
        extends LiveAttributeMap<IFACE_TYPE, IMPL_TYPE>
        implements GridAttributes<IFACE_TYPE> {

    /**
     * @param initialAttributes The attributes map to use until mutated, or else {@code null} to allocate a new one
     */
    protected BaseGridAttributes(@Nullable final Map<String, Object> initialAttributes) {
        super(initialAttributes);
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE restrictSortTo(@NotNull final String... allowedSortingColumns) {
        checkAvailableColumns(Arrays.asList(allowedSortingColumns));
        return withAttributes(Map.of(
                GridAttributes.SORTABLE_COLUMNS_ATTRIBUTE, String.join(",", allowedSortingColumns)));
    }

    protected Collection<String> getSortableColumns() {
        final String sortingRestrictions = (String) getAttribute(SORTABLE_COLUMNS_ATTRIBUTE);
        if (sortingRestrictions == null) {
            return null;
        }
        return Stream.of(sortingRestrictions.split(",")).collect(Collectors.toList());
    }

    protected void setSortableColumns(@NotNull final Collection<String> allowedSortingColumns) {
        checkAvailableColumns(allowedSortingColumns);
        setAttribute(SORTABLE_COLUMNS_ATTRIBUTE, String.join(",", allowedSortingColumns));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE clearSortingRestrictions() {
        return withoutAttributes(List.of(SORTABLE_COLUMNS_ATTRIBUTE));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withDescription(@NotNull final String description) {
        return withAttributes(Map.of(DESCRIPTION_ATTRIBUTE, description));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withColumnDescription(@NotNull final String column, @NotNull final String description) {
        return withColumnDescriptions(Collections.singletonMap(column, description));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withColumnDescriptions(@NotNull final Map<String, String> descriptions) {
        checkAvailableColumns(descriptions.keySet());
        // noinspection unchecked
        final Map<String, String> existingDescriptions =
                (Map<String, String>) getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        if (existingDescriptions != null && existingDescriptions.entrySet().containsAll(descriptions.entrySet())) {
            return prepareReturnThis();
        }
        final Map<String, Object> resultDescriptions =
                existingDescriptions == null ? new HashMap<>(descriptions.size()) : new HashMap<>(existingDescriptions);
        resultDescriptions.putAll(descriptions);
        return withAttributes(Map.of(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, resultDescriptions));
    }

    protected void setColumnDescriptions(@NotNull final Map<String, String> descriptions) {
        checkAvailableColumns(descriptions.keySet());
        return setAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE, descriptions));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE setLayoutHints(@NotNull final String hints) {
        return withAttributes(Map.of(Table.LAYOUT_HINTS_ATTRIBUTE, hints));
    }

    /**
     * Check this grid to ensure that all {@code columns} are present.
     *
     * @param columns The column names to check
     * @throws NoSuchColumnException If any columns were missing
     */
    protected abstract void checkAvailableColumns(@NotNull Collection<String> columns);
}
