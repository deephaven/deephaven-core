/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import gnu.trove.map.hash.TIntObjectHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.utils.DBTimeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;

/**
 * An ingester that replicates a Apache Kafka topic to a Deephaven Table Writer.
 *
 * <p>
 * Each KafkaIngester is assigned a topic and a subset of Kafka partitions. Each Kafka partition is mapped to a
 * Deephaven internal partition. The column partition can be set through the constructor, or defaults to
 * {@link DBTimeUtils#currentDateNy()}.
 * </p>
 *
 * <p>
 * Automatic partition assignment and rebalancing are not supported. Each Kafka ingester instance must uniquely control
 * its checkpoint record, which is incompatible with rebalancing.
 * </p>
 *
 * 
 */
public class KafkaIngester {
    private static final int REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerForClassWithDefault(
            KafkaIngester.class, "reportIntervalMs", 60_000);
    private static final long MAX_ERRS = Configuration.getInstance().getLongForClassWithDefault(
            KafkaIngester.class, "maxErrs", 500);
    private final KafkaConsumer<?, ?> consumer;
    @NotNull
    private final Logger log;
    private final String topic;
    private final String partitionDescription;
    private final IntFunction<KafkaStreamConsumer> partitionToConsumer;
    private final TIntObjectHashMap<KafkaStreamConsumer> consumers = new TIntObjectHashMap<>();
    private final Set<KafkaStreamConsumer> uniqueConsumers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final String logPrefix;
    private long messagesProcessed = 0;
    private long messagesWithErr = 0;
    private long lastProcessed = 0;
    private final Set<TopicPartition> openPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
     *
     * 
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
     *
     * 
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
     *
     * 
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
     * @param log a log for output
     * @param props the properties used to create the {@link KafkaConsumer}
     * @param topic the topic to replicate
     * @param partitionToConsumer a function implementing a mapping from partition to its consumer of records.
     * @param partitionToInitialSeekOffset a function implementing a mapping from partition to its intial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    public KafkaIngester(final Logger log,
            final Properties props,
            final String topic,
            final IntFunction<KafkaStreamConsumer> partitionToConsumer,
            final IntToLongFunction partitionToInitialSeekOffset) {
        this(log, props, topic, ALL_PARTITIONS, partitionToConsumer, partitionToInitialSeekOffset);
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
     * @param log a log for output
     * @param props the properties used to create the {@link KafkaConsumer}
     * @param topic the topic to replicate
     * @param partitionFilter a predicate indicating which partitions we should replicate
     * @param partitionToConsumer a function implementing a mapping from partition to its consumer of records.
     * @param partitionToInitialSeekOffset a function implementing a mapping from partition to its intial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    @SuppressWarnings("rawtypes")
    public KafkaIngester(@NotNull final Logger log,
            final Properties props,
            final String topic,
            final IntPredicate partitionFilter,
            final IntFunction<KafkaStreamConsumer> partitionToConsumer,
            final IntToLongFunction partitionToInitialSeekOffset) {
        this.log = log;
        this.topic = topic;
        this.partitionDescription = partitionFilter.toString();
        this.partitionToConsumer = partitionToConsumer;
        this.logPrefix = KafkaIngester.class.getSimpleName() + "(" + topic + ", " + partitionDescription + "): ";
        consumer = new KafkaConsumer(props);

        final List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        partitions.stream().filter(pi -> partitionFilter.test(pi.partition()))
                .map(pi -> new TopicPartition(topic, pi.partition())).forEach(openPartitions::add);

        consumer.assign(openPartitions);

        final Set<TopicPartition> assignments = consumer.assignment();
        if (assignments.size() <= 0) {
            throw new UncheckedDeephavenException("Empty partition assignments");
        }
        log.info().append(logPrefix).append("Partition Assignments: ").append(assignments.toString()).endl();

        if (assignments.size() != openPartitions.size()) {
            throw new UncheckedDeephavenException(logPrefix + "Partition assignments do not match request: assignments="
                    + assignments + ", request=" + openPartitions);
        }

        for (final TopicPartition topicPartition : assignments) {
            final long seekOffset = partitionToInitialSeekOffset.applyAsLong(topicPartition.partition());
            if (seekOffset == SEEK_TO_BEGINNING) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to beginning.")
                        .append(seekOffset).endl();
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            } else if (seekOffset == SEEK_TO_END) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to end.")
                        .append(seekOffset).endl();
                consumer.seekToEnd(Collections.singletonList(topicPartition));
            } else if (seekOffset != DONT_SEEK) {
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to offset ")
                        .append(seekOffset).append(".").endl();
                consumer.seek(topicPartition, seekOffset);
            }
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
        final int expectedPartitionCount = openPartitions.size();
        final DecimalFormat rateFormat = new DecimalFormat("#.0000");
        while (true) {
            final int currentPartitionSize = openPartitions.size();
            if (currentPartitionSize != expectedPartitionCount) {
                log.error().append(logPrefix)
                        .append("Stopping due to partition size change to ").append(currentPartitionSize)
                        .append(".").endl();
                break;
            }
            final long beforePoll = System.nanoTime();
            final long nextReport = lastReportNanos + reportIntervalNanos;
            final long remainingNanos = beforePoll > nextReport ? 0 : (nextReport - beforePoll);
            boolean noMore = pollOnce(Duration.ofNanos(remainingNanos));
            if (noMore) {
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
        log.info().append(logPrefix).append("Closing Kafka consumer.").endl();
        consumer.close();
        throw new UncheckedDeephavenException("Kafka stream was closed.");
    }

    /**
     *
     * @param timeout
     * @return true if we should abort the consumer thread.
     */
    private boolean pollOnce(final Duration timeout) {
        final ConsumerRecords<?, ?> records;
        try {
            records = consumer.poll(timeout);
        } catch (WakeupException we) {
            // we interpret this as a signal to stop.
            return false;
        } catch (Exception ex) {
            log.error().append(logPrefix).append("Exception while polling for Kafka messages:").append(ex)
                    .append(", aborting.");
            return false;
        }

        for (final TopicPartition topicPartition : records.partitions()) {
            final int partition = topicPartition.partition();

            KafkaStreamConsumer consumer;
            consumer = consumers.get(partition);
            if (consumer == null) {
                consumer = partitionToConsumer.apply(partition);
                uniqueConsumers.add(consumer);
                consumers.put(partition, consumer);
            }


            final List<? extends ConsumerRecord<?, ?>> partitionRecords = records.records(topicPartition);

            try {
                consumer.accept(partitionRecords);
            } catch (Exception ex) {
                ++messagesWithErr;
                log.error().append(logPrefix).append("Exception while processing Kafka message:").append(ex);
                if (messagesWithErr > MAX_ERRS) {
                    consumer.acceptFailure(ex);
                    log.error().append(logPrefix)
                            .append("Max number of errors exceeded, aborting " + this + " consumer thread.");
                    return true;
                }
                continue;
            }
            messagesProcessed += partitionRecords.size();
        }
        return false;
    }
}
