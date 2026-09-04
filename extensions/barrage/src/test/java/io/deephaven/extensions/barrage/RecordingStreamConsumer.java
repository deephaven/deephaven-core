//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.stream.StreamConsumer;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link StreamConsumer} that retains the chunks handed to it so that a test may assert on the values a
 * {@link io.deephaven.stream.StreamPublisher} produced, without involving an update graph.
 */
final class RecordingStreamConsumer implements StreamConsumer {

    private final List<WritableChunk<Values>[]> batches = new ArrayList<>();
    private Throwable failure;

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Values>... data) {
        batches.add(data);
    }

    @Override
    public void accept(@NotNull final Collection<WritableChunk<Values>[]> data) {
        batches.addAll(data);
    }

    @Override
    public void acceptFailure(@NotNull final Throwable cause) {
        failure = cause;
    }

    int batchCount() {
        return batches.size();
    }

    Throwable failure() {
        return failure;
    }

    /**
     * @return the only batch accepted so far, failing if there was not exactly one
     */
    WritableChunk<Values>[] onlyBatch() {
        if (failure != null) {
            throw new AssertionError("Publisher reported a failure", failure);
        }
        if (batches.size() != 1) {
            throw new AssertionError("Expected exactly one batch, found " + batches.size());
        }
        return batches.get(0);
    }

    long longAt(final int column, final int row) {
        return onlyBatch()[column].asWritableLongChunk().get(row);
    }

    String stringAt(final int column, final int row) {
        return onlyBatch()[column].<String>asWritableObjectChunk().get(row);
    }
}
