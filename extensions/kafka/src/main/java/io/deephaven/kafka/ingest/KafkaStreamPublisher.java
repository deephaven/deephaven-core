/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.unboxer.ChunkUnboxer;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.kafka.StreamPublisherBase;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * An adapter that maps keys and values, possibly each with multiple fields, to single Deephaven columns. Each Kafka
 * record produces one Deephaven row.
 */
public class KafkaStreamPublisher extends StreamPublisherBase implements ConsumerRecordToStreamPublisherAdapter {

    private final Runnable shutdownCallback;
    private final int kafkaPartitionColumnIndex;
    private final int offsetColumnIndex;
    private final int timestampColumnIndex;
    private final int simpleKeyColumnIndex;
    private final int simpleValueColumnIndex;

    private final boolean keyIsSimpleObject;
    private final boolean valueIsSimpleObject;

    private final KeyOrValueProcessor keyProcessor;
    private final KeyOrValueProcessor valueProcessor;
    private final Function<Object, Object> keyToChunkObjectMapper;
    private final Function<Object, Object> valueToChunkObjectMapper;

    private KafkaStreamPublisher(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Runnable shutdownCallback,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final KeyOrValueProcessor keyProcessor,
            final KeyOrValueProcessor valueProcessor,
            final int simpleKeyColumnIndex,
            final int simpleValueColumnIndex,
            final Function<Object, Object> keyToChunkObjectMapper,
            final Function<Object, Object> valueToChunkObjectMapper) {
        super(tableDefinition);
        this.shutdownCallback = shutdownCallback;
        this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
        this.offsetColumnIndex = offsetColumnIndex;
        this.timestampColumnIndex = timestampColumnIndex;
        this.simpleKeyColumnIndex = simpleKeyColumnIndex;
        this.simpleValueColumnIndex = simpleValueColumnIndex;
        this.keyProcessor = keyProcessor;
        this.valueProcessor = valueProcessor;
        this.keyToChunkObjectMapper = keyToChunkObjectMapper;
        this.valueToChunkObjectMapper = valueToChunkObjectMapper;

        keyIsSimpleObject = this.simpleKeyColumnIndex >= 0;
        if (keyIsSimpleObject && keyProcessor != null) {
            throw new IllegalArgumentException("Simple Key Column Index can not be set when a keyProcessor is set");
        }

        valueIsSimpleObject = this.simpleValueColumnIndex >= 0;
        if (valueIsSimpleObject && valueProcessor != null) {
            throw new IllegalArgumentException("Simple Value Column Index can not be set when a valueProcessor is set");
        }
    }

    public static ConsumerRecordToStreamPublisherAdapter make(
            @NotNull final Parameters parameters,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Runnable shutdownCallback) {
        if ((parameters.getKeyProcessorArg() != null) && (parameters.getSimpleKeyColumnIndexArg() != -1)) {
            throw new IllegalArgumentException("Either keyProcessor != null or simpleKeyColumnIndex != -1");
        }

        if ((parameters.getValueProcessorArg() != null) && (parameters.getSimpleValueColumnIndexArg() != -1)) {
            throw new IllegalArgumentException("Either valueProcessor != null or simpleValueColumnIndex != -1");
        }

        final KeyOrValueProcessor keyProcessor;
        final int simpleKeyColumnIndex;
        if (parameters.getSimpleKeyColumnIndexArg() == -1) {
            keyProcessor = parameters.getKeyProcessorArg();
            simpleKeyColumnIndex = -1;
        } else {
            final Pair<KeyOrValueProcessor, Integer> keyPair =
                    getProcessorAndSimpleIndex(
                            parameters.getSimpleKeyColumnIndexArg(),
                            parameters.getColumnIndexToChunkType().apply(parameters.getSimpleKeyColumnIndexArg()));
            keyProcessor = keyPair.first;
            simpleKeyColumnIndex = keyPair.second;
        }

        final KeyOrValueProcessor valueProcessor;
        final int simpleValueColumnIndex;
        if (parameters.getSimpleValueColumnIndexArg() == -1) {
            valueProcessor = parameters.getValueProcessorArg();
            simpleValueColumnIndex = -1;
        } else {
            final Pair<KeyOrValueProcessor, Integer> valuePair =
                    getProcessorAndSimpleIndex(
                            parameters.getSimpleValueColumnIndexArg(),
                            parameters.getColumnIndexToChunkType().apply(parameters.getSimpleValueColumnIndexArg()));
            valueProcessor = valuePair.first;
            simpleValueColumnIndex = valuePair.second;
        }

        return new KafkaStreamPublisher(
                tableDefinition,
                shutdownCallback,
                parameters.getKafkaPartitionColumnIndex(),
                parameters.getOffsetColumnIndex(),
                parameters.getTimestampColumnIndex(),
                keyProcessor,
                valueProcessor,
                simpleKeyColumnIndex,
                simpleValueColumnIndex,
                parameters.getKeyToChunkObjectMapper(),
                parameters.getValueToChunkObjectMapper());
    }

    @NotNull
    private static Pair<KeyOrValueProcessor, Integer> getProcessorAndSimpleIndex(int columnIndex, ChunkType chunkType) {
        final boolean isSimpleObject = chunkType == ChunkType.Object;
        final int simpleIndex;
        final KeyOrValueProcessor processor;
        if (!isSimpleObject) {
            processor = new SimpleKeyOrValueProcessor(columnIndex, ChunkUnboxer.getEmptyUnboxer(chunkType));
            simpleIndex = -1;
        } else {
            processor = null;
            simpleIndex = columnIndex;
        }
        return new Pair<>(processor, simpleIndex);
    }

    private boolean haveKey() {
        return !keyIsSimpleObject && keyProcessor != null;
    }

    private boolean haveValue() {
        return !valueIsSimpleObject && valueProcessor != null;
    }

    @Override
    public void propagateFailure(@NotNull final Throwable cause) {
        consumer.acceptFailure(cause);
    }

    @Override
    public synchronized long consumeRecords(@NotNull final List<? extends ConsumerRecord<?, ?>> records) {
        WritableChunk<Values>[] chunks = getChunksToFill();
        checkChunkSizes(chunks);
        int remaining = chunks[0].capacity() - chunks[0].size();

        final int chunkSize = Math.min(records.size(), chunks[0].capacity());

        long bytesProcessed = 0;
        try (final WritableObjectChunk<Object, Values> keyChunkCloseable = haveKey()
                ? WritableObjectChunk.makeWritableChunk(chunkSize)
                : null;
                final WritableObjectChunk<Object, Values> valueChunkCloseable = haveValue()
                        ? WritableObjectChunk.makeWritableChunk(chunkSize)
                        : null) {
            WritableObjectChunk<Object, Values> keyChunk;
            if (keyChunkCloseable != null) {
                keyChunkCloseable.setSize(0);
                keyChunk = keyChunkCloseable;
            } else if (keyIsSimpleObject) {
                keyChunk = chunks[simpleKeyColumnIndex].asWritableObjectChunk();
            } else {
                keyChunk = null;
            }
            WritableObjectChunk<Object, Values> valueChunk;
            if (valueChunkCloseable != null) {
                valueChunkCloseable.setSize(0);
                valueChunk = valueChunkCloseable;
            } else if (valueIsSimpleObject) {
                valueChunk = chunks[simpleValueColumnIndex].asWritableObjectChunk();
            } else {
                valueChunk = null;
            }

            WritableIntChunk<Values> partitionChunk = (kafkaPartitionColumnIndex >= 0)
                    ? chunks[kafkaPartitionColumnIndex].asWritableIntChunk()
                    : null;
            WritableLongChunk<Values> offsetChunk = offsetColumnIndex >= 0
                    ? chunks[offsetColumnIndex].asWritableLongChunk()
                    : null;
            WritableLongChunk<Values> timestampChunk = timestampColumnIndex >= 0
                    ? chunks[timestampColumnIndex].asWritableLongChunk()
                    : null;

            for (ConsumerRecord<?, ?> record : records) {
                if (--remaining == 0) {
                    if (keyChunk != null) {
                        flushKeyChunk(keyChunk, chunks);
                    }
                    if (valueChunk != null) {
                        flushValueChunk(valueChunk, chunks);
                    }

                    checkChunkSizes(chunks);
                    flush();

                    chunks = getChunksToFill();
                    checkChunkSizes(chunks);

                    remaining = chunks[0].capacity() - chunks[0].size();
                    Assert.gtZero(remaining, "remaining");

                    if (kafkaPartitionColumnIndex >= 0) {
                        partitionChunk = chunks[kafkaPartitionColumnIndex].asWritableIntChunk();
                    } else {
                        partitionChunk = null;
                    }
                    if (offsetColumnIndex >= 0) {
                        offsetChunk = chunks[offsetColumnIndex].asWritableLongChunk();
                    } else {
                        offsetChunk = null;
                    }
                    if (timestampColumnIndex >= 0) {
                        timestampChunk = chunks[timestampColumnIndex].asWritableLongChunk();
                    } else {
                        timestampChunk = null;
                    }
                    if (keyIsSimpleObject) {
                        keyChunk = chunks[simpleKeyColumnIndex].asWritableObjectChunk();
                    }
                    if (valueIsSimpleObject) {
                        valueChunk = chunks[simpleValueColumnIndex].asWritableObjectChunk();
                    }
                }


                if (partitionChunk != null) {
                    partitionChunk.add(record.partition());
                }
                if (offsetChunk != null) {
                    offsetChunk.add(record.offset());
                }
                if (timestampChunk != null) {
                    final long timestamp = record.timestamp();
                    if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                        timestampChunk.add(QueryConstants.NULL_LONG);
                    } else {
                        timestampChunk.add(DateTimeUtils.millisToNanos(timestamp));
                    }
                }

                if (keyChunk != null) {
                    keyChunk.add(keyToChunkObjectMapper.apply(record.key()));
                    final int keyBytes = record.serializedKeySize();
                    if (keyBytes > 0) {
                        bytesProcessed += keyBytes;
                    }
                }
                if (valueChunk != null) {
                    valueChunk.add(valueToChunkObjectMapper.apply(record.value()));
                    final int valueBytes = record.serializedValueSize();
                    if (valueBytes > 0) {
                        bytesProcessed += valueBytes;
                    }
                }
            }
            if (keyChunk != null) {
                flushKeyChunk(keyChunk, chunks);
            }
            if (valueChunk != null) {
                flushValueChunk(valueChunk, chunks);
            }

            checkChunkSizes(chunks);
        }
        return bytesProcessed;
    }

    private void checkChunkSizes(WritableChunk<Values>[] chunks) {
        for (int cc = 1; cc < chunks.length; ++cc) {
            if (chunks[cc].size() != chunks[0].size()) {
                throw new IllegalStateException("Publisher chunks have size mismatch: "
                        + Arrays.stream(chunks).map(c -> Integer.toString(c.size())).collect(Collectors.joining(", ")));
            }
        }
    }

    static class SimpleKeyOrValueProcessor implements KeyOrValueProcessor {
        final int offset;
        final ChunkUnboxer.UnboxerKernel unboxer;

        SimpleKeyOrValueProcessor(int offset, ChunkUnboxer.UnboxerKernel unboxer) {
            this.offset = offset;
            this.unboxer = unboxer;
        }

        @Override
        public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
            final WritableChunk<Values> publisherChunk = publisherChunks[offset];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            unboxer.unboxTo(inputChunk, publisherChunk, 0, existingSize);
        }
    }

    void flushKeyChunk(WritableObjectChunk<Object, Values> objectChunk,
            WritableChunk<Values>[] publisherChunks) {
        if (keyIsSimpleObject) {
            return;
        }
        keyProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }

    void flushValueChunk(WritableObjectChunk<Object, Values> objectChunk, WritableChunk<Values>[] publisherChunks) {
        if (valueIsSimpleObject) {
            return;
        }
        valueProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }

    @Override
    public void shutdown() {
        shutdownCallback.run();
    }

    public static class Parameters {

        @NotNull
        private final IntFunction<ChunkType> columnIndexToChunkType;
        private final int kafkaPartitionColumnIndex;
        private final int offsetColumnIndex;
        private final int timestampColumnIndex;
        private final KeyOrValueProcessor keyProcessorArg;
        private final KeyOrValueProcessor valueProcessorArg;
        private final int simpleKeyColumnIndexArg;
        private final int simpleValueColumnIndexArg;
        private final Function<Object, Object> keyToChunkObjectMapper;
        private final Function<Object, Object> valueToChunkObjectMapper;

        public Parameters(
                @NotNull final IntFunction<ChunkType> columnIndexToChunkType,
                final int kafkaPartitionColumnIndex,
                final int offsetColumnIndex,
                final int timestampColumnIndex,
                final KeyOrValueProcessor keyProcessorArg,
                final KeyOrValueProcessor valueProcessorArg,
                final int simpleKeyColumnIndexArg,
                final int simpleValueColumnIndexArg,
                final Function<Object, Object> keyToChunkObjectMapper,
                final Function<Object, Object> valueToChunkObjectMapper) {
            this.columnIndexToChunkType = columnIndexToChunkType;
            this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
            this.offsetColumnIndex = offsetColumnIndex;
            this.timestampColumnIndex = timestampColumnIndex;
            this.keyProcessorArg = keyProcessorArg;
            this.valueProcessorArg = valueProcessorArg;
            this.simpleKeyColumnIndexArg = simpleKeyColumnIndexArg;
            this.simpleValueColumnIndexArg = simpleValueColumnIndexArg;
            this.keyToChunkObjectMapper = keyToChunkObjectMapper;
            this.valueToChunkObjectMapper = valueToChunkObjectMapper;
        }

        @NotNull
        public IntFunction<ChunkType> getColumnIndexToChunkType() {
            return columnIndexToChunkType;
        }

        public int getKafkaPartitionColumnIndex() {
            return kafkaPartitionColumnIndex;
        }

        public int getOffsetColumnIndex() {
            return offsetColumnIndex;
        }

        public int getTimestampColumnIndex() {
            return timestampColumnIndex;
        }

        public KeyOrValueProcessor getKeyProcessorArg() {
            return keyProcessorArg;
        }

        public KeyOrValueProcessor getValueProcessorArg() {
            return valueProcessorArg;
        }

        public int getSimpleKeyColumnIndexArg() {
            return simpleKeyColumnIndexArg;
        }

        public int getSimpleValueColumnIndexArg() {
            return simpleValueColumnIndexArg;
        }

        public Function<Object, Object> getKeyToChunkObjectMapper() {
            return keyToChunkObjectMapper;
        }

        public Function<Object, Object> getValueToChunkObjectMapper() {
            return valueToChunkObjectMapper;
        }
    }
}
