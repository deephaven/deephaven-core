/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import io.deephaven.util.annotations.FinalDefault;
import org.apache.parquet.column.statistics.Statistics;

import java.io.IOException;
import java.nio.IntBuffer;

public interface ColumnWriter extends SafeCloseable {
    /**
     * Add a page with no nulls to the file. Does not track statistics, so the caller must track statistics in another
     * way.
     */
    @FinalDefault
    default void addPageNoNulls(@NotNull Object pageData, int valuesCount) throws IOException {
        addPageNoNulls(pageData, valuesCount, NullStatistics.INSTANCE);
    }

    /**
     * Add a page with no nulls to the file.
     */
    void addPageNoNulls(@NotNull Object pageData, int valuesCount, @NotNull Statistics<?> statistics)
            throws IOException;

    /**
     * Add a dictionary page to the file.
     */
    void addDictionaryPage(@NotNull Object dictionaryValues, int valuesCount) throws IOException;

    /**
     * Add a page (potentially containing nulls) to the file. Does not track statistics, so the caller must track
     * statistics in another way.
     */
    @FinalDefault
    default void addPage(@NotNull Object pageData, int valuesCount) throws IOException {
        addPage(pageData, valuesCount, NullStatistics.INSTANCE);
    }

    /**
     * Add a page (potentially containing nulls) to the file.
     */
    void addPage(Object pageData, int valuesCount, Statistics<?> statistics) throws IOException;

    /**
     * Add a vector page to the file.. Does not track statistics, so the caller must track statistics in another way.
     */
    @FinalDefault
    default void addVectorPage(@NotNull Object pageData,
            @NotNull IntBuffer repeatCount,
            int valuesCount)
            throws IOException {
        addVectorPage(pageData, repeatCount, valuesCount, NullStatistics.INSTANCE);
    }

    /**
     * Add a vector page to the file.
     */
    void addVectorPage(@NotNull Object pageData,
            @NotNull IntBuffer repeatCount,
            int valuesCount,
            @NotNull Statistics<?> statistics)
            throws IOException;

    /**
     * Reset the statistics for this column. This needs to be done between row groups
     */
    void resetStats();

    /**
     * Return the current statistics.
     */
    Statistics<?> getStats();
}
