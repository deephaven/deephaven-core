//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;

import java.io.IOException;
import java.util.List;

/**
 * GWT super-source stub for {@code DictionaryChunkWriter}.
 * <p>
 * Dictionary-encoded (DE) writing is <b>not supported</b> in the GWT/browser environment. This stub exists solely to
 * allow {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} to compile under GWT. In particular,
 * {@code instanceof DictionaryChunkWriter} checks in {@code BarrageMessageWriterImpl} compile correctly and always
 * evaluate to {@code false} at runtime because no instances of this class are ever created in GWT.
 * None of these methods will ever be called.
 */
public class DictionaryChunkWriter implements ChunkWriter<Chunk<Values>> {

    @Override
    public Context makeContext(final Chunk<Values> chunk, final long rowOffset) {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    @Override
    public DrainableColumn getInputStream(
            final Context context,
            final RowSet subset,
            final BarrageOptions options) throws IOException {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    @Override
    public DrainableColumn getEmptyInputStream(final BarrageOptions options) throws IOException {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    @Override
    public boolean isFieldNullable() {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public long getDictId() {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public ChunkWriter<Chunk<Values>> getValuesWriter() {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public ChunkType getValuesChunkType() {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public DrainableColumn getInputStream(
            final Context context,
            final RowSet subset,
            final BarrageOptions options,
            final DictionaryWriterState state) throws IOException {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public static WritableChunk<Values> buildDeltaValuesChunk(
            final ChunkType valuesChunkType,
            final List<Object> deltaValues) {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }

    public DrainableColumn getEmptyIndexStream(final BarrageOptions options) throws IOException {
        throw new UnsupportedOperationException("Dictionary encoding is not supported in GWT");
    }
}
