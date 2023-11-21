/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class AsyncErrorStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofTime("Time"),
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofInt("OperationNumber"),
            ColumnDefinition.ofString("Description"),
            ColumnDefinition.ofLong("SourceQueryEvaluationNumber"),
            ColumnDefinition.ofInt("SourceQueryOperationNumber"),
            ColumnDefinition.ofString("SourceQueryDescription"),
            ColumnDefinition.of("Cause", Type.ofCustom(Throwable.class)));

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    AsyncErrorStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(
            long timeNanos,
            long evaluationNumber,
            int operationNumber,
            String description,
            long sourceQueryEvaluationNumber,
            int sourceQueryOperationNumber,
            String sourceQueryDescription,
            Throwable cause) {
        chunks[0].asWritableLongChunk().add(timeNanos);
        chunks[1].asWritableLongChunk().add(evaluationNumber);
        chunks[2].asWritableIntChunk().add(operationNumber);
        chunks[3].<String>asWritableObjectChunk().add(description);
        chunks[4].asWritableLongChunk().add(sourceQueryEvaluationNumber);
        chunks[5].asWritableIntChunk().add(sourceQueryOperationNumber);
        chunks[6].<String>asWritableObjectChunk().add(sourceQueryDescription);
        chunks[7].<Throwable>asWritableObjectChunk().add(cause);
        if (chunks[0].size() == CHUNK_SIZE) {
            flushInternal();
        }
    }

    @Override
    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }

    @Override
    public void shutdown() {}
}
