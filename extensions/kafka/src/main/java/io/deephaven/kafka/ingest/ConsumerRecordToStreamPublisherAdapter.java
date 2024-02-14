/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * Converter from a stream of Kafka records to a Deephaven StreamPublisher.
 */
public interface ConsumerRecordToStreamPublisherAdapter extends StreamPublisher {

    /**
     * Propagate a failure from the Kafka consumer to this StreamPublisher.
     *
     * @param cause The failure to propagate
     */
    void propagateFailure(@NotNull Throwable cause);

    /**
     * Consume a List of Kafka records, producing zero or more rows in the output.
     *
     * @param receiveTime the time, in nanoseconds since the epoch, the records were received in this process
     * @param records the records received from {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)}.
     * @return the number of bytes processed
     * @throws IOException if there was an error writing to the output table
     */
    long consumeRecords(long receiveTime, @NotNull List<? extends ConsumerRecord<?, ?>> records) throws IOException;
}
