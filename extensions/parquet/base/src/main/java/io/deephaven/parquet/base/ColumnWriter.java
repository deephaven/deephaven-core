/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.IntBuffer;

public interface ColumnWriter extends SafeCloseable {
    void addPageNoNulls(@NotNull Object pageData, int valuesCount) throws IOException;

    void addDictionaryPage(@NotNull Object dictionaryValues, int valuesCount) throws IOException;

    void addPage(@NotNull Object pageData, int valuesCount) throws IOException;

    void addVectorPage(@NotNull Object pageData, @NotNull IntBuffer repeatCount, int valuesCount) throws IOException;
}
