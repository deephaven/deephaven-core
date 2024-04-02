//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.unboxer.ChunkUnboxer;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.kafka.StreamPublisherBase;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An adapter that maps keys and values, possibly each with multiple fields, to single Deephaven columns. Each Kafka
 * record produces one Deephaven row.
 */
public class KafkaStreamPublisher extends StreamPublisherBase implements ConsumerRecordToStreamPublisherAdapter {

    public static final int NULL_COLUMN_INDEX = -1;

    private final Runnable shutdownCallback;
    private final int kafkaPartitionColumnIndex;
    private final int offsetColumnIndex;
    private final int timestampColumnIndex;
    private final int simpleKeyColumnIndex;
    private final int simpleValueColumnIndex;

    private final int receiveTimeColumnIndex;
    private final int keyBytesColumnIndex;
    private final int valueBytesColumnIndex;

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
            final Function<Object, Object> valueToChunkObjectMapper,
            final int receiveTimeColumnIndex,
            final int keyBytesColumnIndex,
            final int valueBytesColumnIndex) {
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
        this.receiveTimeColumnIndex = receiveTimeColumnIndex;
        this.keyBytesColumnIndex = keyBytesColumnIndex;
        this.valueBytesColumnIndex = valueBytesColumnIndex;

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
            @NotNull final Runnable shutdownCallback) {

        final KeyOrValueProcessor keyProcessor;
        final int simpleKeyColumnIndex;
        if (parameters.getSimpleKeyColumnIndex() == NULL_COLUMN_INDEX) {
            keyProcessor = parameters.getKeyProcessor();
            simpleKeyColumnIndex = NULL_COLUMN_INDEX;
        } else {
            final Pair<KeyOrValueProcessor, Integer> keyPair =
                    getProcessorAndSimpleIndex(
                            parameters.getSimpleKeyColumnIndex(),
                            StreamChunkUtils.chunkTypeForColumnIndex(
                                    parameters.getTableDefinition(),
                                    parameters.getSimpleKeyColumnIndex()));
            keyProcessor = keyPair.first;
            simpleKeyColumnIndex = keyPair.second;
        }

        final KeyOrValueProcessor valueProcessor;
        final int simpleValueColumnIndex;
        if (parameters.getSimpleValueColumnIndex() == NULL_COLUMN_INDEX) {
            valueProcessor = parameters.getValueProcessor();
            simpleValueColumnIndex = NULL_COLUMN_INDEX;
        } else {
            final Pair<KeyOrValueProcessor, Integer> valuePair =
                    getProcessorAndSimpleIndex(
                            parameters.getSimpleValueColumnIndex(),
                            StreamChunkUtils.chunkTypeForColumnIndex(
                                    parameters.getTableDefinition(),
                                    parameters.getSimpleValueColumnIndex()));
            valueProcessor = valuePair.first;
            simpleValueColumnIndex = valuePair.second;
        }

        return new KafkaStreamPublisher(
                parameters.getTableDefinition(),
                shutdownCallback,
                parameters.getKafkaPartitionColumnIndex(),
                parameters.getOffsetColumnIndex(),
                parameters.getTimestampColumnIndex(),
                keyProcessor,
                valueProcessor,
                simpleKeyColumnIndex,
                simpleValueColumnIndex,
                parameters.getKeyToChunkObjectMapper(),
                parameters.getValueToChunkObjectMapper(),
                parameters.getReceiveTimeColumnIndex(),
                parameters.getKeyBytesColumnIndex(),
                parameters.getValueBytesColumnIndex());
    }

    @NotNull
    private static Pair<KeyOrValueProcessor, Integer> getProcessorAndSimpleIndex(int columnIndex, ChunkType chunkType) {
        final boolean isSimpleObject = chunkType == ChunkType.Object;
        final int simpleIndex;
        final KeyOrValueProcessor processor;
        if (!isSimpleObject) {
            processor = new SimpleKeyOrValueProcessor(columnIndex, ChunkUnboxer.getEmptyUnboxer(chunkType));
            simpleIndex = NULL_COLUMN_INDEX;
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
    public synchronized long consumeRecords(long receiveTime,
            @NotNull final List<? extends ConsumerRecord<?, ?>> records) {
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

            WritableLongChunk<Values> receiveTimeChunk = receiveTimeColumnIndex >= 0
                    ? chunks[receiveTimeColumnIndex].asWritableLongChunk()
                    : null;

            WritableIntChunk<Values> keyBytesChunk = keyBytesColumnIndex >= 0
                    ? chunks[keyBytesColumnIndex].asWritableIntChunk()
                    : null;

            WritableIntChunk<Values> valueBytesChunk = valueBytesColumnIndex >= 0
                    ? chunks[valueBytesColumnIndex].asWritableIntChunk()
                    : null;

            for (ConsumerRecord<?, ?> record : records) {
                if (remaining == 0) {
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
                    if (receiveTimeColumnIndex >= 0) {
                        receiveTimeChunk = chunks[receiveTimeColumnIndex].asWritableLongChunk();
                    } else {
                        receiveTimeChunk = null;
                    }
                    if (keyBytesColumnIndex >= 0) {
                        keyBytesChunk = chunks[keyBytesColumnIndex].asWritableIntChunk();
                    } else {
                        keyBytesChunk = null;
                    }
                    if (valueBytesColumnIndex >= 0) {
                        valueBytesChunk = chunks[valueBytesColumnIndex].asWritableIntChunk();
                    } else {
                        valueBytesChunk = null;
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
                if (receiveTimeChunk != null) {
                    receiveTimeChunk.add(receiveTime);
                }
                final int keyBytes = record.serializedKeySize();
                if (keyBytesChunk != null) {
                    keyBytesChunk.add(keyBytes >= 0 ? keyBytes : QueryConstants.NULL_INT);
                }
                final int valueBytes = record.serializedValueSize();
                if (valueBytesChunk != null) {
                    valueBytesChunk.add(valueBytes >= 0 ? valueBytes : QueryConstants.NULL_INT);
                }

                if (keyChunk != null) {
                    keyChunk.add(keyToChunkObjectMapper.apply(record.key()));
                    if (keyBytes > 0) {
                        bytesProcessed += keyBytes;
                    }
                }
                if (valueChunk != null) {
                    valueChunk.add(valueToChunkObjectMapper.apply(record.value()));
                    if (valueBytes > 0) {
                        bytesProcessed += valueBytes;
                    }
                }

                --remaining;
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
        private final TableDefinition tableDefinition;
        private final int kafkaPartitionColumnIndex;
        private final int offsetColumnIndex;
        private final int timestampColumnIndex;
        private final int receiveTimeColumnIndex;
        private final int keyBytesColumnIndex;
        private final int valueBytesColumnIndex;
        private final KeyOrValueProcessor keyProcessor;
        private final KeyOrValueProcessor valueProcessor;
        private final int simpleKeyColumnIndex;
        private final int simpleValueColumnIndex;
        private final Function<Object, Object> keyToChunkObjectMapper;
        private final Function<Object, Object> valueToChunkObjectMapper;

        private Parameters(
                @NotNull final TableDefinition tableDefinition,
                final int kafkaPartitionColumnIndex,
                final int offsetColumnIndex,
                final int timestampColumnIndex,
                final KeyOrValueProcessor keyProcessor,
                final KeyOrValueProcessor valueProcessor,
                final int simpleKeyColumnIndex,
                final int simpleValueColumnIndex,
                final Function<Object, Object> keyToChunkObjectMapper,
                final Function<Object, Object> valueToChunkObjectMapper,
                int receiveTimeColumnIndex,
                int keyBytesColumnIndex,
                int valueBytesColumnIndex) {
            this.tableDefinition = tableDefinition;
            this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
            this.offsetColumnIndex = offsetColumnIndex;
            this.timestampColumnIndex = timestampColumnIndex;
            this.keyProcessor = keyProcessor;
            this.valueProcessor = valueProcessor;
            this.simpleKeyColumnIndex = simpleKeyColumnIndex;
            this.simpleValueColumnIndex = simpleValueColumnIndex;
            this.keyToChunkObjectMapper = keyToChunkObjectMapper;
            this.valueToChunkObjectMapper = valueToChunkObjectMapper;
            this.receiveTimeColumnIndex = receiveTimeColumnIndex;
            this.keyBytesColumnIndex = keyBytesColumnIndex;
            this.valueBytesColumnIndex = valueBytesColumnIndex;
        }

        @NotNull
        public TableDefinition getTableDefinition() {
            return tableDefinition;
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

        public KeyOrValueProcessor getKeyProcessor() {
            return keyProcessor;
        }

        public KeyOrValueProcessor getValueProcessor() {
            return valueProcessor;
        }

        public int getSimpleKeyColumnIndex() {
            return simpleKeyColumnIndex;
        }

        public int getSimpleValueColumnIndex() {
            return simpleValueColumnIndex;
        }

        public int getReceiveTimeColumnIndex() {
            return receiveTimeColumnIndex;
        }

        public int getKeyBytesColumnIndex() {
            return keyBytesColumnIndex;
        }

        public int getValueBytesColumnIndex() {
            return valueBytesColumnIndex;
        }

        public Function<Object, Object> getKeyToChunkObjectMapper() {
            return keyToChunkObjectMapper;
        }

        public Function<Object, Object> getValueToChunkObjectMapper() {
            return valueToChunkObjectMapper;
        }

        public static Builder builder() {
            return new Builder();
        }

        @SuppressWarnings("UnusedReturnValue")
        public static class Builder {

            private TableDefinition tableDefinition;
            private int kafkaPartitionColumnIndex = NULL_COLUMN_INDEX;
            private int offsetColumnIndex = NULL_COLUMN_INDEX;
            private int timestampColumnIndex = NULL_COLUMN_INDEX;
            private int receiveTimeColumnIndex = NULL_COLUMN_INDEX;
            private int keyBytesColumnIndex = NULL_COLUMN_INDEX;
            private int valueBytesColumnIndex = NULL_COLUMN_INDEX;
            private KeyOrValueProcessor keyProcessor;
            private KeyOrValueProcessor valueProcessor;
            private int simpleKeyColumnIndex = NULL_COLUMN_INDEX;
            private int simpleValueColumnIndex = NULL_COLUMN_INDEX;
            private Function<Object, Object> keyToChunkObjectMapper = Function.identity();
            private Function<Object, Object> valueToChunkObjectMapper = Function.identity();

            private Builder() {}

            public Builder setTableDefinition(@NotNull final TableDefinition tableDefinition) {
                this.tableDefinition = tableDefinition;
                return this;
            }

            public Builder setKafkaPartitionColumnIndex(final int kafkaPartitionColumnIndex) {
                this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
                return this;
            }

            public Builder setOffsetColumnIndex(final int offsetColumnIndex) {
                this.offsetColumnIndex = offsetColumnIndex;
                return this;
            }

            public Builder setTimestampColumnIndex(final int timestampColumnIndex) {
                this.timestampColumnIndex = timestampColumnIndex;
                return this;
            }

            public Builder setReceiveTimeColumnIndex(final int receiveTimeColumnIndex) {
                this.receiveTimeColumnIndex = receiveTimeColumnIndex;
                return this;
            }

            public Builder setKeyBytesColumnIndex(final int keyBytesColumnIndex) {
                this.keyBytesColumnIndex = keyBytesColumnIndex;
                return this;
            }

            public Builder setValueBytesColumnIndex(final int valueBytesColumnIndex) {
                this.valueBytesColumnIndex = valueBytesColumnIndex;
                return this;
            }

            public Builder setKeyProcessor(final KeyOrValueProcessor keyProcessor) {
                this.keyProcessor = keyProcessor;
                return this;
            }

            public Builder setValueProcessor(final KeyOrValueProcessor valueProcessor) {
                this.valueProcessor = valueProcessor;
                return this;
            }

            public Builder setSimpleKeyColumnIndex(final int simpleKeyColumnIndex) {
                this.simpleKeyColumnIndex = simpleKeyColumnIndex;
                return this;
            }

            public Builder setSimpleValueColumnIndex(final int simpleValueColumnIndex) {
                this.simpleValueColumnIndex = simpleValueColumnIndex;
                return this;
            }

            public Builder setKeyToChunkObjectMapper(@NotNull final Function<Object, Object> keyToChunkObjectMapper) {
                this.keyToChunkObjectMapper = keyToChunkObjectMapper;
                return this;
            }

            public Builder setValueToChunkObjectMapper(
                    @NotNull final Function<Object, Object> valueToChunkObjectMapper) {
                this.valueToChunkObjectMapper = valueToChunkObjectMapper;
                return this;
            }

            public KafkaStreamPublisher.Parameters build() {
                if (keyProcessor != null && simpleKeyColumnIndex >= 0) {
                    throw new IllegalArgumentException("Only one of keyProcessor or simpleKeyColumnIndex may be set");
                }

                if (valueProcessor != null && simpleValueColumnIndex >= 0) {
                    throw new IllegalArgumentException(
                            "Only one of valueProcessor or simpleValueColumnIndex may be set");
                }
                return new KafkaStreamPublisher.Parameters(
                        tableDefinition,
                        kafkaPartitionColumnIndex,
                        offsetColumnIndex,
                        timestampColumnIndex,
                        keyProcessor,
                        valueProcessor,
                        simpleKeyColumnIndex,
                        simpleValueColumnIndex,
                        keyToChunkObjectMapper,
                        valueToChunkObjectMapper,
                        receiveTimeColumnIndex,
                        keyBytesColumnIndex,
                        valueBytesColumnIndex);
            }
        }
    }
}
