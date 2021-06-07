package io.deephaven.kafka.ingest;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Duration;

/**
 * Converter from a stream of Kafka records to a table writer.
 *
 * Typically, this should be created by a factory that takes a {@link io.deephaven.tablelogger.TableWriter} as its input.
 */
@FunctionalInterface
public interface ConsumerRecordToTableWriterAdapter {
    /**
     * Consume a Kafka record, producing zero or more rows in the output.
     *
     * @param record the record received from {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)}.
     * @throws IOException if there was an error writing to the output table
     */
    void consumeRecord(ConsumerRecord<?, ?> record) throws IOException;
}
