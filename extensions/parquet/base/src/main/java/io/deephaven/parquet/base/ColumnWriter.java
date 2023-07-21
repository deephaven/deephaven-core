/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.util.SafeCloseable;

import java.io.IOException;
import java.nio.IntBuffer;

public interface ColumnWriter extends SafeCloseable {
    void addPageNoNulls(Object pageData, int valuesCount) throws IOException;

    void addDictionaryPage(Object dictionaryValues, int valuesCount) throws IOException;

    void addPage(Object pageData, int valuesCount) throws IOException;

    void addVectorPage(Object pageData, IntBuffer repeatCount, int valuesCount) throws IOException;
}
