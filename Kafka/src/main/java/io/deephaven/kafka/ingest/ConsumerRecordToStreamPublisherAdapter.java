package io.deephaven.kafka.ingest;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * Converter from a stream of Kafka records to a Deephaven StreamPublisher.
 */
@FunctionalInterface
public interface ConsumerRecordToStreamPublisherAdapter {
    /**
     * Consume a List of Kafka records, producing zero or more rows in the output.
     *
     * @param records the records received from
     *        {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)}.
     * @throws IOException if there was an error writing to the output table
     */
    void consumeRecords(List<? extends ConsumerRecord<?, ?>> records) throws IOException;
}
