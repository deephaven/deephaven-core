//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;

import java.nio.ByteBuffer;

/**
 * A {@link PageMaterializerFactory} that reads PLAIN-encoded BINARY pages with its own {@link ValuesReader} rather than
 * parquet's. Implementing this is opt-in and narrowing: the reader supplied here is handed only to the materializers
 * this same factory builds.
 */
public interface PlainBinaryPageReaderFactory extends PageMaterializerFactory {

    /**
     * @param in a heap-backed page buffer, positioned past the repetition and definition levels
     */
    ValuesReader makePlainBinaryValuesReader(ByteBuffer in);
}
