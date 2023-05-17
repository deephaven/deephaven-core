/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import org.jetbrains.annotations.NotNull;

/**
 * Implementations of this interface are able to compute groupings.
 */
public interface GroupingProvider {
    String INDEX_DIR_PREFIX = "Index-";
    String INDEX_COL_NAME = "Index";

    /**
     * Get a {@link GroupingBuilder} suitable for creating groups with specific properties.
     * 
     * @return a {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder getGroupingBuilder();

    /**
     * Check if this provider is able to create a grouping or not.
     * 
     * @return true if this provider can create a grouping.
     */
    boolean hasGrouping();
}
