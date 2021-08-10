package io.deephaven.kafka.ingest;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkUnboxer;
import io.deephaven.kafka.StreamPublisherImpl;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An adapter that maps keys and values to single Deephaven columns.  Each Kafka record produces one Deephaven row.
 */
public class SimpleConsumerRecordToStreamPublisherAdapter implements ConsumerRecordToStreamPublisherAdapter {
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

    private SimpleConsumerRecordToStreamPublisherAdapter(
            final StreamPublisherImpl publisher,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final KeyOrValueProcessor keyProcessor,
            final KeyOrValueProcessor valueProcessor,
            final int simpleKeyColumnIndex,
            final int simpleValueColumnIndex) {
        this.publisher = publisher;
        this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
        this.offsetColumnIndex = offsetColumnIndex;
        this.timestampColumnIndex = timestampColumnIndex;
        this.simpleKeyColumnIndex = simpleKeyColumnIndex;
        this.simpleValueColumnIndex = simpleValueColumnIndex;
        this.keyProcessor = keyProcessor;
        this.valueProcessor = valueProcessor;

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
            final StreamPublisherImpl publisher,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final int keyColumnIndex,
            final int valueColumnIndex) {

        if (valueColumnIndex < 0) {
            throw new IllegalArgumentException("Value column index must be non-negative: " + valueColumnIndex);
        }
        final ChunkType keyChunkType = publisher.chunkType(keyColumnIndex);
        final ChunkType valueChunkType = publisher.chunkType(valueColumnIndex);

        final Pair<KeyOrValueProcessor, Integer> keyPair = getProcessorAndSimpleIndex(keyColumnIndex, keyChunkType);
        final Pair<KeyOrValueProcessor, Integer> valuePair = getProcessorAndSimpleIndex(valueColumnIndex, valueChunkType);

        return new SimpleConsumerRecordToStreamPublisherAdapter(
                publisher, kafkaPartitionColumnIndex, offsetColumnIndex, timestampColumnIndex, keyPair.first, valuePair.first, keyPair.second, valuePair.second);
    }

    public static ConsumerRecordToStreamPublisherAdapter make(
            final StreamPublisherImpl publisher,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final KeyOrValueProcessor keyProcessor,
            final KeyOrValueProcessor valueProcessor,
            final int simpleKeyColumnIndex,
            final int simpleValueColumnIndex
            ) {
        if (valueProcessor == null) {
            throw new IllegalArgumentException("Value processor is required!");
        }

        return new SimpleConsumerRecordToStreamPublisherAdapter(
                publisher, kafkaPartitionColumnIndex, offsetColumnIndex, timestampColumnIndex, keyProcessor, valueProcessor, simpleKeyColumnIndex, simpleValueColumnIndex);
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

    @SuppressWarnings("unchecked")
    @Override
    public void consumeRecords(List<? extends ConsumerRecord<?, ?>> records) {
        WritableChunk [] chunks = publisher.getChunks();
        int remaining = chunks[0].capacity() - chunks[0].size();

        final int chunkSize = Math.min(records.size(), chunks[0].capacity());

        WritableObjectChunk<Object, Attributes.Values> keyChunk = null;
        WritableObjectChunk<Object, Attributes.Values> valueChunk;

        try (final WritableObjectChunk<Object, Attributes.Values> keyChunkCloseable = !keyIsSimpleObject && simpleKeyColumnIndex >= 0 ? WritableObjectChunk.makeWritableChunk(chunkSize) : null;
             final WritableObjectChunk<Object, Attributes.Values> valueChunkCloseable = !valueIsSimpleObject ? WritableObjectChunk.makeWritableChunk(chunkSize) : null) {

            if (keyChunkCloseable != null) {
                keyChunkCloseable.setSize(0);
                keyChunk = keyChunkCloseable;
            } else if (keyIsSimpleObject) {
                keyChunk = chunks[simpleKeyColumnIndex].asWritableObjectChunk();
            }
            if (valueChunkCloseable != null) {
                valueChunkCloseable.setSize(0);
                valueChunk = valueChunkCloseable;
            } else {
                valueChunk = chunks[simpleValueColumnIndex].asWritableObjectChunk();
            }

            WritableIntChunk<Attributes.Values> partitionChunk = kafkaPartitionColumnIndex >= 0 ? chunks[kafkaPartitionColumnIndex].asWritableIntChunk() : null;
            WritableLongChunk<Attributes.Values> offsetChunk = offsetColumnIndex >= 0 ? chunks[offsetColumnIndex].asWritableLongChunk() : null;
            WritableLongChunk<Attributes.Values> timestampChunk =  timestampColumnIndex >= 0 ? chunks[timestampColumnIndex].asWritableLongChunk() : null;

            for (ConsumerRecord<?, ?> record : records) {
                if (--remaining == 0) {
                    if (keyChunk != null) {
                        flushKeyChunk(keyChunk, chunks);
                    }
                    flushValueChunk(valueChunk, chunks);

                    publisher.flush();

                    chunks = publisher.getChunks();
                    remaining = chunks[0].capacity() - chunks[0].size();
                    Assert.gtZero(remaining, "remaining");

                    if (kafkaPartitionColumnIndex > 0) {
                        partitionChunk = chunks[kafkaPartitionColumnIndex].asWritableIntChunk();
                    } else {
                        partitionChunk = null;
                    }
                    if (offsetColumnIndex > 0) {
                        offsetChunk = chunks[offsetColumnIndex].asWritableLongChunk();
                    } else {
                        offsetChunk = null;
                    }
                    if (timestampColumnIndex > 0) {
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
                    keyChunk.add(record.key());
                }
                valueChunk.add(record.value());
            }
            if (keyChunk != null) {
                flushKeyChunk(keyChunk, chunks);
            }
            flushValueChunk(valueChunk, chunks);

            for (int cc = 1; cc < chunks.length; ++cc) {
                if (chunks[cc].size() != chunks[0].size()) {
                    throw new IllegalStateException("Publisher chunks have size mismatch: " + Arrays.stream(chunks).map(c -> Integer.toString(c.size())).collect(Collectors.joining(", ")));
                }
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
        public void handleChunk(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
            final WritableChunk<Attributes.Values> publisherChunk = publisherChunks[offset];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            unboxer.unboxTo(inputChunk, publisherChunk, 0, existingSize);
        }
    }

    void flushKeyChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
        if (keyIsSimpleObject) {
            return;
        }
        keyProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }

    void flushValueChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
        if (valueIsSimpleObject) {
            return;
        }
        valueProcessor.handleChunk(objectChunk, publisherChunks);
        objectChunk.setSize(0);
    }
}
