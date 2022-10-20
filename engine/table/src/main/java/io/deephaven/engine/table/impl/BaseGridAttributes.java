package io.deephaven.engine.table.impl;

import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.StringUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

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
    public final IFACE_TYPE restrictSortTo(String... allowedSortingColumns) {
        Assert.neqNull(allowedSortingColumns, "allowedSortingColumns");
        checkAvailableColumns(Arrays.asList(allowedSortingColumns));
        return withAttributes(Map.of(
                GridAttributes.SORTABLE_COLUMNS_ATTRIBUTE, StringUtils.joinStrings(allowedSortingColumns, ",")));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE clearSortingRestrictions() {
        return withoutAttributes(List.of(SORTABLE_COLUMNS_ATTRIBUTE));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withDescription(String description) {
        return withAttributes(Map.of(DESCRIPTION_ATTRIBUTE, description));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withColumnDescription(String column, String description) {
        return withColumnDescription(Collections.singletonMap(column, description));
    }

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE withColumnDescription(Map<String, String> descriptions) {
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

    @Override
    @ConcurrentMethod
    public final IFACE_TYPE setLayoutHints(String hints) {
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
