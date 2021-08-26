package io.deephaven.kafka.ingest;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.unboxer.ChunkUnboxer;
import io.deephaven.kafka.StreamPublisherImpl;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An adapter that maps keys and values, possibly each with multiple fields, to single Deephaven
 * columns. Each Kafka record produces one Deephaven row.
 */
public class KafkaStreamPublisher implements ConsumerRecordToStreamPublisherAdapter {
    private final StreamPublisherImpl publisher;
    private final int kafkaPartitionColumnIndex;
    private final int offsetColumnIndex;
    private final int timestampColumnIndex;
    private final int simpleKeyColumnIndex;
    private final int simpleValueColumnIndex;

    private final boolean keyIsSimpleObject;
    private final boolean valueIsSimpleObject;

    final KeyOrValueProcessor keyProcessor;
    final KeyOrValueProcessor valueProcessor;
    final Function<Object, Object> keyToChunkObjectMapper;
    final Function<Object, Object> valueToChunkObjectMapper;

    private KafkaStreamPublisher(
        final StreamPublisherImpl publisher,
        final int kafkaPartitionColumnIndex,
        final int offsetColumnIndex,
        final int timestampColumnIndex,
        final KeyOrValueProcessor keyProcessor,
        final KeyOrValueProcessor valueProcessor,
        final int simpleKeyColumnIndex,
        final int simpleValueColumnIndex,
        final Function<Object, Object> keyToChunkObjectMapper,
        final Function<Object, Object> valueToChunkObjectMapper) {
        this.publisher = publisher;
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
            throw new IllegalArgumentException(
                "Simple Key Column Index can not be set when a keyProcessor is set");
        }

        valueIsSimpleObject = this.simpleValueColumnIndex >= 0;
        if (valueIsSimpleObject && valueProcessor != null) {
            throw new IllegalArgumentException(
                "Simple Value Column Index can not be set when a valueProcessor is set");
        }
    }

    public static ConsumerRecordToStreamPublisherAdapter make(
        final StreamPublisherImpl publisher,
        final int kafkaPartitionColumnIndex,
        final int offsetColumnIndex,
        final int timestampColumnIndex,
        final KeyOrValueProcessor keyProcessorArg,
        final KeyOrValueProcessor valueProcessorArg,
        final int simpleKeyColumnIndexArg,
        final int simpleValueColumnIndexArg,
        final Function<Object, Object> keyToChunkObjectMapper,
        final Function<Object, Object> valueToChunkObjectMapper) {
        if ((keyProcessorArg != null) && (simpleKeyColumnIndexArg != -1)) {
            throw new IllegalArgumentException(
                "Either keyProcessor != null or simpleKeyColumnIndex != -1");
        }

        if ((valueProcessorArg != null) && (simpleValueColumnIndexArg != -1)) {
            throw new IllegalArgumentException(
                "Either valueProcessor != null or simpleValueColumnIndex != -1");
        }

        final KeyOrValueProcessor keyProcessor;
        final int simpleKeyColumnIndex;
        if (simpleKeyColumnIndexArg == -1) {
            keyProcessor = keyProcessorArg;
            simpleKeyColumnIndex = -1;
        } else {
            final Pair<KeyOrValueProcessor, Integer> keyPair =
                getProcessorAndSimpleIndex(
                    simpleKeyColumnIndexArg,
                    publisher.chunkType(simpleKeyColumnIndexArg));
            keyProcessor = keyPair.first;
            simpleKeyColumnIndex = keyPair.second;
        }

        final KeyOrValueProcessor valueProcessor;
        final int simpleValueColumnIndex;
        if (simpleValueColumnIndexArg == -1) {
            valueProcessor = valueProcessorArg;
            simpleValueColumnIndex = -1;
        } else {
            final Pair<KeyOrValueProcessor, Integer> valuePair =
                getProcessorAndSimpleIndex(
                    simpleValueColumnIndexArg,
                    publisher.chunkType(simpleValueColumnIndexArg));
            valueProcessor = valuePair.first;
            simpleValueColumnIndex = valuePair.second;
        }

        return new KafkaStreamPublisher(
            publisher,
            kafkaPartitionColumnIndex,
            offsetColumnIndex,
            timestampColumnIndex,
            keyProcessor,
            valueProcessor,
            simpleKeyColumnIndex,
            simpleValueColumnIndex,
            keyToChunkObjectMapper,
            valueToChunkObjectMapper);
    }

    @NotNull
    private static Pair<KeyOrValueProcessor, Integer> getProcessorAndSimpleIndex(int columnIndex,
        ChunkType chunkType) {
        final boolean isSimpleObject = chunkType == ChunkType.Object;
        final int simpleIndex;
        final KeyOrValueProcessor processor;
        if (!isSimpleObject) {
            processor =
                new SimpleKeyOrValueProcessor(columnIndex, ChunkUnboxer.getEmptyUnboxer(chunkType));
            simpleIndex = -1;
        } else {
            processor = null;
            simpleIndex = columnIndex;
        }
        return new Pair<>(processor, simpleIndex);
    }

    @Override
    public void consumeRecords(List<? extends ConsumerRecord<?, ?>> records) {
        publisher.doLocked(() -> doConsumeRecords(records));
    }

    private boolean haveKey() {
        return !keyIsSimpleObject && keyProcessor != null;
    }

    private boolean haveValue() {
        return !valueIsSimpleObject && valueProcessor != null;
    }

    @SuppressWarnings("unchecked")
    private void doConsumeRecords(List<? extends ConsumerRecord<?, ?>> records) {
        WritableChunk[] chunks = publisher.getChunks();
        checkChunkSizes(chunks);
        int remaining = chunks[0].capacity() - chunks[0].size();

        final int chunkSize = Math.min(records.size(), chunks[0].capacity());

        try (final WritableObjectChunk<Object, Attributes.Values> keyChunkCloseable = haveKey()
            ? WritableObjectChunk.makeWritableChunk(chunkSize)
            : null;
            final WritableObjectChunk<Object, Attributes.Values> valueChunkCloseable = haveValue()
                ? WritableObjectChunk.makeWritableChunk(chunkSize)
                : null) {
            WritableObjectChunk<Object, Attributes.Values> keyChunk;
            if (keyChunkCloseable != null) {
                keyChunkCloseable.setSize(0);
                keyChunk = keyChunkCloseable;
            } else if (keyIsSimpleObject) {
                keyChunk = chunks[simpleKeyColumnIndex].asWritableObjectChunk();
            } else {
                keyChunk = null;
            }
            WritableObjectChunk<Object, Attributes.Values> valueChunk;
            if (valueChunkCloseable != null) {
                valueChunkCloseable.setSize(0);
                valueChunk = valueChunkCloseable;
            } else if (valueIsSimpleObject) {
                valueChunk = chunks[simpleValueColumnIndex].asWritableObjectChunk();
            } else {
                valueChunk = null;
            }

            WritableIntChunk<Attributes.Values> partitionChunk = (kafkaPartitionColumnIndex >= 0)
                ? chunks[kafkaPartitionColumnIndex].asWritableIntChunk()
                : null;
            WritableLongChunk<Attributes.Values> offsetChunk = offsetColumnIndex >= 0
                ? chunks[offsetColumnIndex].asWritableLongChunk()
                : null;
            WritableLongChunk<Attributes.Values> timestampChunk = timestampColumnIndex >= 0
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
                    publisher.flush();

                    chunks = publisher.getChunks();
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
                        timestampChunk.add(DBTimeUtils.millisToNanos(timestamp));
                    }
                }

                if (keyChunk != null) {
                    keyChunk.add(keyToChunkObjectMapper.apply(record.key()));
                }
                if (valueChunk != null) {
                    valueChunk.add(valueToChunkObjectMapper.apply(record.value()));
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
    }

    private void checkChunkSizes(WritableChunk[] chunks) {
        for (int cc = 1; cc < chunks.length; ++cc) {
            if (chunks[cc].size() != chunks[0].size()) {
                throw new IllegalStateException(
                    "Publisher chunks have size mismatch: " + Arrays.stream(chunks)
                        .map(c -> Integer.toString(c.size())).collect(Collectors.joining(", ")));
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
        public void handleChunk(ObjectChunk<Object, Attributes.Values> inputChunk,
            WritableChunk<Attributes.Values>[] publisherChunks) {
            final WritableChunk<Attributes.Values> publisherChunk = publisherChunks[offset];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            unboxer.unboxTo(inputChunk, publisherChunk, 0, existingSize);
        }
    }

    void flushKeyChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk,
        WritableChunk<Attributes.Values>[] publisherChunks) {
        if (keyIsSimpleObject) {
            return;
        }
        keyProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }

    void flushValueChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk,
        WritableChunk<Attributes.Values>[] publisherChunks) {
        if (valueIsSimpleObject) {
            return;
        }
        valueProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }
}
