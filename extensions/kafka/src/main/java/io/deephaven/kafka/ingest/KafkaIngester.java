//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.io.logger.Logger;
import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.kafka.KafkaTools.InitialOffsetLookup;
import io.deephaven.util.annotations.InternalUseOnly;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;

/**
 * An ingester that consumes an Apache Kafka topic and a subset of its partitions via one or more
 * {@link KafkaRecordConsumer stream consumers}.
 *
 * <p>
 * This class is an internal implementation detail for io.deephaven.kafka; is not intended to be used directly by client
 * code. It lives in a separate package as a means of code organization.
 */
@InternalUseOnly
public class KafkaIngester {
    private static final int REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerForClassWithDefault(
            KafkaIngester.class, "reportIntervalMs", 60_000);
    private static final long MAX_ERRS = Configuration.getInstance().getLongForClassWithDefault(
            KafkaIngester.class, "maxErrs", 0);

    private final Logger log;
    private final String topic;
    private final String partitionDescription;
    private final String logPrefix;
    private final KafkaConsumer<?, ?> kafkaConsumer;

    private final TIntObjectHashMap<KafkaRecordConsumer> streamConsumers = new TIntObjectHashMap<>();
    private final KeyedIntObjectHashMap<TopicPartition> assignedPartitions =
            new KeyedIntObjectHashMap<>(new KeyedIntObjectKey.BasicStrict<>() {
                @Override
                public int getIntKey(@NotNull final TopicPartition topicPartition) {
                    return topicPartition.partition();
                }
            });

    @Nullable
    private final ConsumerLoopCallback consumerLoopCallback;

    private long messagesProcessed = 0;
    private long bytesProcessed = 0;
    private long pollCalls = 0;
    private long messagesWithErr = 0;
    private long lastMessages = 0;
    private long lastBytes = 0;
    private long lastPollCalls = 0;

    private volatile boolean needsAssignment;
    private volatile boolean done;

    /**
     * Constant predicate that returns true for all partitions. This is the default, each and every partition that
     * exists will be handled by the same ingester. Because Kafka consumers are inherently single threaded, to scale
     * beyond what a single consumer can handle, you must create multiple consumers each with a subset of partitions
     * using {@link PartitionRange}, {@link PartitionRoundRobin}, {@link SinglePartition} or a custom
     * {@link IntPredicate}.
     */
    public static final IntPredicate ALL_PARTITIONS = new IntPredicate() {
        @Override
        public boolean test(int value) {
            return true;
        }

        @Override
        public String toString() {
            return "ALL";
        }
    };

    /**
     * A predicate for handling a range of partitions.
     */
    public static class PartitionRange implements IntPredicate {
        final int startInclusive;
        final int endInclusive;

        /**
         * Create a predicate for a range of partitions.
         *
         * @param startInclusive the first partition for an ingester
         * @param endInclusive the last partition for an ingester
         */
        public PartitionRange(int startInclusive, int endInclusive) {
            this.startInclusive = startInclusive;
            this.endInclusive = Require.geq(endInclusive, "endInclusive", startInclusive, "startInclusive");
        }

        @Override
        public boolean test(int value) {
            return value >= startInclusive && value <= endInclusive;
        }

        @Override
        public String toString() {
            return Integer.toString(startInclusive) + (startInclusive == endInclusive ? "" : endInclusive);
        }
    }

    /**
     * A predicate for handling a single partition.
     */
    public static class SinglePartition extends PartitionRange {
        /**
         * Create a predicate for a single partition.
         *
         * @param partition the partition to ingest
         */
        public SinglePartition(int partition) {
            super(partition, partition);
        }
    }

    /**
     * A predicate for evenly distributing partitions among a set of ingesters.
     */
    public static class PartitionRoundRobin implements IntPredicate {
        final int consumerIndex;
        final int consumerCount;

        /**
         * Creates a predicate for evenly distributing partitions among a set of ingesters.
         *
         * @param consumerIndex the index of this consumer; you should have precisely one ingester configured for each
         *        index between zero (inclusive) and consumerCount (exclusive)
         * @param consumerCount the number of consumers that will ingest this topic
         */
        public PartitionRoundRobin(int consumerIndex, int consumerCount) {
            this.consumerIndex = Require.geqZero(
                    Require.lt(consumerIndex, "consumerIndex", consumerCount, "consumerCount"), "consumerIndex");
            this.consumerCount = consumerCount;
        }

        @Override
        public boolean test(int value) {
            return value % consumerCount == consumerIndex;
        }

        @Override
        public String toString() {
            return "N % " + consumerCount + " == " + consumerIndex;
        }
    }


    public static final long SEEK_TO_BEGINNING = -1;
    public static final long DONT_SEEK = -2;
    public static final long SEEK_TO_END = -3;
    public static final IntToLongFunction ALL_PARTITIONS_SEEK_TO_BEGINNING = (int p) -> SEEK_TO_BEGINNING;
    public static final IntToLongFunction ALL_PARTITIONS_DONT_SEEK = (int p) -> DONT_SEEK;
    public static final IntToLongFunction ALL_PARTITIONS_SEEK_TO_END = (int p) -> SEEK_TO_END;

    /**
     * Creates a Kafka ingester for the given topic.
     *
     * @param log A log for output
     * @param props The properties used to create the {@link KafkaConsumer}
     * @param topic The topic to replicate
     * @param partitionFilter A predicate indicating which partitions we should replicate
     * @param partitionToStreamConsumer A function implementing a mapping from partition to its consumer of records. The
     *        function will be invoked once per partition at construction; implementations should internally defer
     *        resource allocation until first call to {@link KafkaRecordConsumer#consume(long, List)} or
     *        {@link KafkaRecordConsumer#acceptFailure(Throwable)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     * @param keyDeserializer, the key deserializer, see
     *        {@link KafkaConsumer#KafkaConsumer(Properties, Deserializer, Deserializer)}
     * @param valueDeserializer, the value deserializer, see
     *        {@link KafkaConsumer#KafkaConsumer(Properties, Deserializer, Deserializer)}
     * @param consumerLoopCallback the consumer loop callback
     */
    public KafkaIngester(
            @NotNull final Logger log,
            @NotNull final Properties props,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final Function<TopicPartition, KafkaRecordConsumer> partitionToStreamConsumer,
            @NotNull final InitialOffsetLookup partitionToInitialSeekOffset,
            @NotNull final Deserializer<?> keyDeserializer,
            @NotNull final Deserializer<?> valueDeserializer,
            @Nullable final ConsumerLoopCallback consumerLoopCallback) {
        this.log = log;
        this.topic = topic;
        partitionDescription = partitionFilter.toString();
        logPrefix = KafkaIngester.class.getSimpleName() + "(" + topic + ", " + partitionDescription + "): ";
        kafkaConsumer = new KafkaConsumer<>(props,
                Objects.requireNonNull(keyDeserializer),
                Objects.requireNonNull(valueDeserializer));
        this.consumerLoopCallback = consumerLoopCallback;

        kafkaConsumer.partitionsFor(topic).stream().filter(pi -> partitionFilter.test(pi.partition()))
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .forEach(tp -> {
                    assignedPartitions.add(tp);
                    streamConsumers.put(tp.partition(), partitionToStreamConsumer.apply(tp));
                });
        assign();

        for (final TopicPartition topicPartition : assignedPartitions) {
            final long seekOffset =
                    partitionToInitialSeekOffset.getInitialOffset(kafkaConsumer, topicPartition);
            if (seekOffset == SEEK_TO_BEGINNING) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to beginning.")
                        .append(seekOffset).endl();
                kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            } else if (seekOffset == SEEK_TO_END) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to end.")
                        .append(seekOffset).endl();
                kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
            } else if (seekOffset != DONT_SEEK) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to offset ")
                        .append(seekOffset).append(".").endl();
                kafkaConsumer.seek(topicPartition, seekOffset);
            }
        }
    }

    private void assign() {
        synchronized (assignedPartitions) {
            kafkaConsumer.assign(assignedPartitions.values());
            log.info().append(logPrefix).append("Partition Assignments: ")
                    .append(assignedPartitions.values().toString())
                    .endl();
        }
    }

    @Override
    public String toString() {
        return KafkaIngester.class.getSimpleName() + topic + ":" + partitionDescription;
    }

    /**
     * Starts a consumer thread which replicates the consumed Kafka messages to Deephaven.
     * <p>
     * This method must not be called more than once on an ingester instance.
     */
    public void start() {
        final Thread t = new Thread(this::consumerLoop, this.toString());
        t.setDaemon(true);
        t.start();
    }

    private static double unitsPerSec(final long units, final long nanos) {
        if (nanos <= 0) {
            return 0;
        }
        return 1000.0 * 1000.0 * 1000.0 * units / nanos;
    }

    private void consumerLoop() {
        final long reportIntervalNanos = REPORT_INTERVAL_MS * 1_000_000L;
        long lastReportNanos = System.nanoTime();
        long nextReport = lastReportNanos + reportIntervalNanos;
        final DecimalFormat rateFormat = new DecimalFormat("#.###");
        while (!done) {
            while (needsAssignment) {
                needsAssignment = false;
                assign();
            }
            final long beforePoll = System.nanoTime();
            final long remainingNanos = beforePoll > nextReport ? 0 : (nextReport - beforePoll);

            boolean more = true;
            if (consumerLoopCallback != null) {
                try {
                    consumerLoopCallback.beforePoll(kafkaConsumer);
                } catch (Exception e) {
                    log.error().append(logPrefix).append("Exception while executing beforePoll callback:").append(e)
                            .append(", aborting.").endl();
                    notifyAllConsumersOnFailure(e);
                    more = false;
                }
            }
            if (more) {
                more = pollOnce(Duration.ofNanos(remainingNanos));
                if (consumerLoopCallback != null) {
                    try {
                        consumerLoopCallback.afterPoll(kafkaConsumer, more);
                    } catch (Exception e) {
                        log.error().append(logPrefix).append("Exception while executing afterPoll callback:").append(e)
                                .append(", aborting.").endl();
                        notifyAllConsumersOnFailure(e);
                        more = false;
                    }
                }
            }
            if (!more) {
                log.error().append(logPrefix)
                        .append("Stopping due to errors (").append(messagesWithErr)
                        .append(" messages with error out of ").append(messagesProcessed).append(" messages processed)")
                        .endl();
                break;
            }
            final long afterPoll = System.nanoTime();
            if (afterPoll > nextReport) {
                final long periodMessages = messagesProcessed - lastMessages;
                final long periodBytes = bytesProcessed - lastBytes;
                final long periodPolls = pollCalls - lastPollCalls;
                final long periodNanos = afterPoll - lastReportNanos;
                log.info().append(logPrefix)
                        .append("ingestion period summary")
                        .append(": polls=").append(periodPolls)
                        .append(", messages=").append(periodMessages)
                        .append(", bytes=").append(periodBytes)
                        .append(", time=").append(periodNanos / 1000_000L).append("ms")
                        .append(", polls/sec=").append(rateFormat.format(unitsPerSec(periodPolls, periodNanos)))
                        .append(", msgs/sec=").append(rateFormat.format(unitsPerSec(periodMessages, periodNanos)))
                        .append(", bytes/sec=").append(rateFormat.format(unitsPerSec(periodBytes, periodNanos)))
                        .endl();
                lastReportNanos = afterPoll;
                nextReport = lastReportNanos + reportIntervalNanos;
                lastMessages = messagesProcessed;
                lastBytes = bytesProcessed;
                lastPollCalls = pollCalls;
            }
        }
        log.info().append(logPrefix).append("Closing Kafka consumer").endl();
        kafkaConsumer.close();
    }

    /**
     * @param timeout Poll timeout duration
     * @return True if we should continue processing messages; false if we should abort the consumer thread.
     */
    private boolean pollOnce(final Duration timeout) {
        final ConsumerRecords<?, ?> records;
        final long receiveTime;
        try {
            ++pollCalls;
            records = kafkaConsumer.poll(timeout);
            receiveTime = Clock.system().currentTimeNanos();
        } catch (WakeupException we) {
            // we interpret a wakeup as a signal to stop /this/ poll.
            return true;
        } catch (Exception ex) {
            log.error().append(logPrefix).append("Exception while polling for Kafka messages:").append(ex)
                    .append(", aborting.").endl();
            notifyAllConsumersOnFailure(ex);
            return false;
        }

        for (final TopicPartition topicPartition : records.partitions()) {
            final int partition = topicPartition.partition();

            final KafkaRecordConsumer streamConsumer;
            synchronized (streamConsumers) {
                streamConsumer = streamConsumers.get(partition);
            }
            if (streamConsumer == null) {
                continue;
            }

            final List<? extends ConsumerRecord<?, ?>> partitionRecords = records.records(topicPartition);
            if (partitionRecords.isEmpty()) {
                continue;
            }

            try {
                bytesProcessed += streamConsumer.consume(receiveTime, partitionRecords);
            } catch (Throwable ex) {
                ++messagesWithErr;
                log.error().append(logPrefix).append("Exception while processing Kafka message:").append(ex).endl();
                /*
                 * TODO (https://github.com/deephaven/deephaven-core/issues/4147): If we ignore any errors, we may have
                 * misaligned chunks due to partially consumed records. Harden the record-parsing code against this
                 * scenario.
                 */
                if (messagesWithErr > MAX_ERRS) {
                    log.error().append(logPrefix)
                            .append("Max number of errors exceeded, aborting " + this + " consumer thread.")
                            .endl();
                    streamConsumer.acceptFailure(ex);
                    return false;
                }
                continue;
            }
            messagesProcessed += partitionRecords.size();
        }
        return true;
    }

    private void notifyAllConsumersOnFailure(Exception ex) {
        final KafkaRecordConsumer[] allConsumers;
        synchronized (streamConsumers) {
            allConsumers = streamConsumers.valueCollection().toArray(KafkaRecordConsumer[]::new);
        }
        for (final KafkaRecordConsumer streamConsumer : allConsumers) {
            streamConsumer.acceptFailure(ex);
        }
    }

    public void shutdown() {
        if (done) {
            return;
        }
        synchronized (streamConsumers) {
            streamConsumers.clear();
        }
        done = true;
        kafkaConsumer.wakeup();
    }

    public void shutdownPartition(final int partition) {
        if (done) {
            return;
        }
        final boolean becameEmpty;
        synchronized (streamConsumers) {
            if (streamConsumers.remove(partition) == null) {
                return;
            }
            becameEmpty = streamConsumers.isEmpty();
        }
        assignedPartitions.remove(partition);
        if (becameEmpty) {
            done = true;
        } else {
            needsAssignment = true;
        }
        kafkaConsumer.wakeup();
    }
}
