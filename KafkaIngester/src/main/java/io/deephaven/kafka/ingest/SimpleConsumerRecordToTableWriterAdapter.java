package io.deephaven.kafka.ingest;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkUnboxer;
import io.deephaven.kafka.StreamPublisherImpl;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;

/**
 * An adapter that maps keys and values to single Deephaven columns.  Each Kafka record produces one Deephaven row.
 */
public class SimpleConsumerRecordToTableWriterAdapter implements ConsumerRecordToTableWriterAdapter {
    private final StreamPublisherImpl publisher;
    private final int kafkaPartitionColumnIndex;
    private final int offsetColumnIndex;
    private final int timestampColumnIndex;
    private final int keyColumnIndex;
    private final int valueColumnIndex;

    private final boolean keyIsSimpleObject;
    private final boolean valueIsSimpleObject;

    final KeyOrValueProcessor keyProcessor;
    final KeyOrValueProcessor valueProcessor;

    private SimpleConsumerRecordToTableWriterAdapter(
            final StreamPublisherImpl publisher,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final int keyColumnIndex,
            final int valueColumnIndex) {
        this.publisher = publisher;
        this.kafkaPartitionColumnIndex = kafkaPartitionColumnIndex;
        this.offsetColumnIndex = offsetColumnIndex;
        this.timestampColumnIndex = timestampColumnIndex;
        this.keyColumnIndex = keyColumnIndex;
        this.valueColumnIndex = valueColumnIndex;
        if (valueColumnIndex < 0) {
            throw new IllegalArgumentException("Value column index must be non-negative: " + valueColumnIndex);
        }
        final ChunkType keyChunkType = publisher.chunkType(keyColumnIndex);
        final ChunkType valueChunkType = publisher.chunkType(valueColumnIndex);

        keyIsSimpleObject = keyChunkType == ChunkType.Object;
        if (!keyIsSimpleObject) {
            keyProcessor = new SimpleKeyOrValueProcessor(keyColumnIndex, ChunkUnboxer.getEmptyUnboxer(keyChunkType));
        } else {
            keyProcessor = null;
        }

        valueIsSimpleObject = valueChunkType == ChunkType.Object;
        if (!valueIsSimpleObject) {
            valueProcessor = new SimpleKeyOrValueProcessor(valueColumnIndex, ChunkUnboxer.getEmptyUnboxer(valueChunkType));
        } else {
            valueProcessor = null;
        }
    }

    /*
     * Create a {@link ConsumerRecordToTableWriterAdapter} that maps simple keys and values to single columns in a
     * Deephaven table.  Each Kafka record becomes a row in the table's output.
     *
     * @param kafkaPartitionColumnName  the name of the Integer column representing the Kafka partition, if null the partition
     *                                  is not mapped to a Deephaven column
     * @param offsetColumnName          the name of the Long column representing the Kafka offset, if null the offset is not
     *                                  mapped to a Deephaven column
     * @param timestampColumnName       the name of the DateTime column representing the Kafka partition, if null the
     *                                  partition is not mapped to a Deephaven column
     * @param keyColumnName             the name of the Deephaven column for the record's key
     * @param valueColumnName           the name of the Deephaven column for the record's value
     *
     * @return an adapter for the TableWriter
     */
//    public static Function<TableWriter<?>, ConsumerRecordToTableWriterAdapter> makeFactory(
//            final String kafkaPartitionColumnName,
//            final String offsetColumnName,
//            final String timestampColumnName,
//            final String keyColumnName,
//            @NotNull final String valueColumnName
//    ) {
//        return (TableWriter<?> tw) -> new SimpleConsumerRecordToTableWriterAdapter(
//                tw, kafkaPartitionColumnName, offsetColumnName, timestampColumnName, keyColumnName, valueColumnName);
//    }

    public static ConsumerRecordToTableWriterAdapter make(
            final StreamPublisherImpl publisher,
            final int kafkaPartitionColumnIndex,
            final int offsetColumnIndex,
            final int timestampColumnIndex,
            final int keyColumnIndex,
            final int valueColumnIndex) {
        return new SimpleConsumerRecordToTableWriterAdapter(
                publisher, kafkaPartitionColumnIndex, offsetColumnIndex, timestampColumnIndex, keyColumnIndex, valueColumnIndex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void consumeRecords(List<? extends ConsumerRecord<?, ?>> records) {
        WritableChunk [] chunks = publisher.getChunks();
        int remaining = chunks[0].capacity() - chunks[0].size();

        final int chunkSize = Math.min(records.size(), chunks[0].capacity());

        WritableObjectChunk<Object, Attributes.Values> keyChunk = null;
        WritableObjectChunk<Object, Attributes.Values> valueChunk;

        try (final WritableObjectChunk<Object, Attributes.Values> keyChunkCloseable = !keyIsSimpleObject && keyColumnIndex >= 0 ? WritableObjectChunk.makeWritableChunk(chunkSize) : null;
             final WritableObjectChunk<Object, Attributes.Values> valueChunkCloseable = !valueIsSimpleObject ? WritableObjectChunk.makeWritableChunk(chunkSize) : null) {

            if (keyChunkCloseable != null) {
                keyChunkCloseable.setSize(0);
                keyChunk = keyChunkCloseable;
            } else if (keyIsSimpleObject) {
                keyChunk = chunks[keyColumnIndex].asWritableObjectChunk();
            }
            if (valueChunkCloseable != null) {
                valueChunkCloseable.setSize(0);
                valueChunk = valueChunkCloseable;
            } else {
                valueChunk = chunks[valueColumnIndex].asWritableObjectChunk();
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
                        keyChunk = chunks[keyColumnIndex].asWritableObjectChunk();
                    }
                    if (valueIsSimpleObject) {
                        valueChunk = chunks[valueColumnIndex].asWritableObjectChunk();
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
        }
    }

    interface KeyOrValueProcessor {
        /**
         * After consuming a set of generic records for a batch that are not raw objects, we pass the keys or values to
         * an appropriate handler.  The handler must know it's data types and offsets within the publisher chunks, and
         * "copy" the data from the inputChunk to the appropriate chunks for the stream publisher.
         *
         * @param inputChunk      the chunk containing the keys or values as Kafka deserialized them from the consumer record
         * @param publisherChunks the output chunks for this table that must be appended to.
         */
        void handleChunk(WritableObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> [] publisherChunks);
    }

    static class SimpleKeyOrValueProcessor implements KeyOrValueProcessor {
        final int offset;
        final ChunkUnboxer.UnboxerKernel unboxer;

        SimpleKeyOrValueProcessor(int offset, ChunkUnboxer.UnboxerKernel unboxer) {
            this.offset = offset;
            this.unboxer = unboxer;
        }

        @Override
        public void handleChunk(WritableObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
            final WritableChunk<Attributes.Values> publisherChunk = publisherChunks[offset];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            unboxer.unboxTo(inputChunk, publisherChunk, 0, existingSize);
            inputChunk.setSize(0);
        }
    }

    void flushKeyChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
        if (keyIsSimpleObject) {
            return;
        }
        keyProcessor.handleChunk(objectChunk, publisherChunks);
    }

    void flushValueChunk(WritableObjectChunk<Object, Attributes.Values> objectChunk, WritableChunk<Attributes.Values> [] publisherChunks) {
        if (valueIsSimpleObject) {
            return;
        }
        valueProcessor.handleChunk(objectChunk, publisherChunks);
    }
}
