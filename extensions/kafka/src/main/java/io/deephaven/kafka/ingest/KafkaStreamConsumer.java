package io.deephaven.kafka.ingest;

import io.deephaven.stream.StreamFailureConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;

/**
 * Consumer for lists of ConsumerRecords coming from Kafka. The StreamFailureConsumer is extended so that we can report
 * errors emanating from our consumer thread.
 */
public interface KafkaStreamConsumer extends Consumer<List<? extends ConsumerRecord<?, ?>>>, StreamFailureConsumer {
}
