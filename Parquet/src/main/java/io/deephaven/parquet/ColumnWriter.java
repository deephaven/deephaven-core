package io.deephaven.parquet;

import java.io.IOException;
import java.nio.IntBuffer;

public interface ColumnWriter {
    void addPageNoNulls(Object pageData, int valuesCount) throws IOException;

    void addDictionaryPage(Object dictionaryValues, int valuesCount) throws IOException;

    void addPage(Object pageData, Object nullValues, int valuesCount) throws IOException;

    void addVectorPage(Object pageData, IntBuffer repeatCount, int valuesCount, Object nullValue) throws IOException;

    void close();
}
