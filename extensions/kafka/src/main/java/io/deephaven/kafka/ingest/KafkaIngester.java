/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.io.logger.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;

/**
 * An ingester that consumes an Apache Kafka topic and a subset of its partitions via one or more
 * {@link KafkaStreamConsumer stream consumers}.
 */
public class KafkaIngester {
    private static final int REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerForClassWithDefault(
            KafkaIngester.class, "reportIntervalMs", 60_000);
    private static final long MAX_ERRS = Configuration.getInstance().getLongForClassWithDefault(
            KafkaIngester.class, "maxErrs", 500);
    private final KafkaConsumer<?, ?> kafkaConsumer;
    @NotNull
    private final Logger log;
    private final String topic;
    private final String partitionDescription;
    private final TIntObjectHashMap<KafkaStreamConsumer> streamConsumers = new TIntObjectHashMap<>();
    private final KeyedIntObjectHashMap<TopicPartition> assignedPartitions =
            new KeyedIntObjectHashMap<>(new KeyedIntObjectKey.BasicStrict<TopicPartition>() {
                @Override
                public int getIntKey(@NotNull final TopicPartition topicPartition) {
                    return topicPartition.partition();
                }
            });
    private final String logPrefix;
    private long messagesProcessed = 0;
    private long messagesWithErr = 0;
    private long lastProcessed = 0;

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

    /**
     * Creates a Kafka ingester for all partitions of a given topic.
     *
     * @param log A log for output
     * @param props The properties used to create the {@link KafkaConsumer}
     * @param topic The topic to replicate
     * @param partitionToStreamConsumer A function implementing a mapping from partition to its consumer of records. The
     *        function will be invoked once per partition at construction; implementations should internally defer
     *        resource allocation until first call to {@link KafkaStreamConsumer#accept(Object)} or
     *        {@link KafkaStreamConsumer#acceptFailure(Exception)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    public KafkaIngester(final Logger log,
            final Properties props,
            final String topic,
            final IntFunction<KafkaStreamConsumer> partitionToStreamConsumer,
            final IntToLongFunction partitionToInitialSeekOffset) {
        this(log, props, topic, ALL_PARTITIONS, partitionToStreamConsumer, partitionToInitialSeekOffset);
    }

    public static long SEEK_TO_BEGINNING = -1;
    public static long DONT_SEEK = -2;
    public static long SEEK_TO_END = -3;
    public static IntToLongFunction ALL_PARTITIONS_SEEK_TO_BEGINNING = (int p) -> SEEK_TO_BEGINNING;
    public static IntToLongFunction ALL_PARTITIONS_DONT_SEEK = (int p) -> DONT_SEEK;
    public static IntToLongFunction ALL_PARTITIONS_SEEK_TO_END = (int p) -> SEEK_TO_END;

    /**
     * Creates a Kafka ingester for the given topic.
     * 
     * @param log A log for output
     * @param props The properties used to create the {@link KafkaConsumer}
     * @param topic The topic to replicate
     * @param partitionFilter A predicate indicating which partitions we should replicate
     * @param partitionToStreamConsumer A function implementing a mapping from partition to its consumer of records. The
     *        function will be invoked once per partition at construction; implementations should internally defer
     *        resource allocation until first call to {@link KafkaStreamConsumer#accept(Object)} or
     *        {@link KafkaStreamConsumer#acceptFailure(Exception)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    @SuppressWarnings("rawtypes")
    public KafkaIngester(@NotNull final Logger log,
            final Properties props,
            final String topic,
            final IntPredicate partitionFilter,
            final IntFunction<KafkaStreamConsumer> partitionToStreamConsumer,
            final IntToLongFunction partitionToInitialSeekOffset) {
        this.log = log;
        this.topic = topic;
        partitionDescription = partitionFilter.toString();
        logPrefix = KafkaIngester.class.getSimpleName() + "(" + topic + ", " + partitionDescription + "): ";
        kafkaConsumer = new KafkaConsumer(props);

        kafkaConsumer.partitionsFor(topic).stream().filter(pi -> partitionFilter.test(pi.partition()))
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .forEach(tp -> {
                    assignedPartitions.add(tp);
                    streamConsumers.put(tp.partition(), partitionToStreamConsumer.apply(tp.partition()));
                });
        assign();

        for (final TopicPartition topicPartition : assignedPartitions) {
            final long seekOffset = partitionToInitialSeekOffset.applyAsLong(topicPartition.partition());
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

    private static double msgPerSec(final long msgs, final long nanos) {
        return 1000.0 * 1000.0 * 1000.0 * msgs / nanos;
    }

    private void consumerLoop() {
        final long reportIntervalNanos = REPORT_INTERVAL_MS * 1_000_000L;
        long lastReportNanos = System.nanoTime();
        final DecimalFormat rateFormat = new DecimalFormat("#.0000");
        while (!done) {
            while (needsAssignment) {
                needsAssignment = false;
                assign();
            }
            final long beforePoll = System.nanoTime();
            final long nextReport = lastReportNanos + reportIntervalNanos;
            final long remainingNanos = beforePoll > nextReport ? 0 : (nextReport - beforePoll);
            boolean more = pollOnce(Duration.ofNanos(remainingNanos));
            if (!more) {
                log.error().append(logPrefix)
                        .append("Stopping due to errors (").append(messagesWithErr)
                        .append(" messages with error out of ").append(messagesProcessed).append(" messages processed)")
                        .endl();
                break;
            }
            final long afterPoll = System.nanoTime();
            if (afterPoll > nextReport) {
                final long intervalMessages = messagesProcessed - lastProcessed;
                final long intervalNanos = afterPoll - lastReportNanos;
                log.info().append(logPrefix)
                        .append("Processed ").append(intervalMessages).append(" in ")
                        .append(intervalNanos / 1000_000L).append("ms, ")
                        .append(rateFormat.format(msgPerSec(intervalMessages, intervalNanos))).append(" msgs/sec")
                        .endl();
                lastReportNanos = afterPoll;
                lastProcessed = messagesProcessed;
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
        try {
            records = kafkaConsumer.poll(timeout);
        } catch (WakeupException we) {
            // we interpret a wakeup as a signal to stop /this/ poll.
            return true;
        } catch (Exception ex) {
            log.error().append(logPrefix).append("Exception while polling for Kafka messages:").append(ex)
                    .append(", aborting.");
            return false;
        }

        for (final TopicPartition topicPartition : records.partitions()) {
            final int partition = topicPartition.partition();

            final KafkaStreamConsumer streamConsumer;
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
                streamConsumer.accept(partitionRecords);
            } catch (Exception ex) {
                ++messagesWithErr;
                log.error().append(logPrefix).append("Exception while processing Kafka message:").append(ex);
                if (messagesWithErr > MAX_ERRS) {
                    streamConsumer.acceptFailure(ex);
                    log.error().append(logPrefix)
                            .append("Max number of errors exceeded, aborting " + this + " consumer thread.");
                    return false;
                }
                continue;
            }
            messagesProcessed += partitionRecords.size();
        }
        return true;
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
