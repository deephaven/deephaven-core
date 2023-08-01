/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
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
import org.jetbrains.annotations.Nullable;

import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;

/**
 * An ingester that consumes an Apache Kafka topic and a subset of its partitions via one or more
 * {@link KafkaRecordConsumer stream consumers}.
 */
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
     * A callback which is invoked from the consumer loop, enabling clients to inject logic to be invoked by the Kafka consumer thread.
     */
    public interface ConsumerLoopCallback {
        /**
         * Called before the consumer is polled for records.
         *
         * @param consumer the KafkaConsumer that will be polled for records
         */
        void beforePoll(KafkaConsumer<?, ?> consumer);

        /**
         * Called after the consumer is polled for records and they have been published to the downstream KafkaRecordConsumer.
         *
         * @param consumer the KafkaConsumer that has been polled for records
         * @param more true if more records should be read, false if the consumer should be shut down due to error
         */
        void afterPoll(KafkaConsumer<?, ?> consumer, boolean more);
    }

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
     *        resource allocation until first call to {@link KafkaRecordConsumer#consume(List)} or
     *        {@link KafkaRecordConsumer#acceptFailure(Throwable)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    public KafkaIngester(
            @NotNull final Logger log,
            @NotNull final Properties props,
            @NotNull final String topic,
            @NotNull final Function<TopicPartition, KafkaRecordConsumer> partitionToStreamConsumer,
            @NotNull final IntToLongFunction partitionToInitialSeekOffset) {
        this(log, props, topic, ALL_PARTITIONS, partitionToStreamConsumer, partitionToInitialSeekOffset);
    }

    /**
     * Creates a Kafka ingester for the given topic.
     *
     * @param log A log for output
     * @param props The properties used to create the {@link KafkaConsumer}
     * @param topic The topic to replicate
     * @param partitionFilter A predicate indicating which partitions we should replicate
     * @param partitionToStreamConsumer A function implementing a mapping from partition to its consumer of records. The
     *        function will be invoked once per partition at construction; implementations should internally defer
     *        resource allocation until first call to {@link KafkaRecordConsumer#consume(List)} or
     *        {@link KafkaRecordConsumer#acceptFailure(Throwable)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    public KafkaIngester(
            @NotNull final Logger log,
            @NotNull final Properties props,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final Function<TopicPartition, KafkaRecordConsumer> partitionToStreamConsumer,
            @NotNull final IntToLongFunction partitionToInitialSeekOffset) {
        this(log, props, topic, partitionFilter, partitionToStreamConsumer, new IntToLongLookupAdapter(partitionToInitialSeekOffset), null);
    }

    /**
     * Determines the initial offset to seek to for a given KafkaConsumer and TopicPartition.
     */
    @FunctionalInterface
    public interface InitialOffsetLookup {
        long getInitialOffset(KafkaConsumer<?, ?> consumer, TopicPartition topicPartition);
    }

    /**
     * Adapts an IntToLongFunction to a PartitionToInitialOffsetFunction by ignoring the topic and consumer parameters.
     */

    public static class IntToLongLookupAdapter implements InitialOffsetLookup {
        private final IntToLongFunction function;

        public IntToLongLookupAdapter(IntToLongFunction function) {
            this.function = function;
        }

        @Override
        public long getInitialOffset(final KafkaConsumer<?, ?> consumer, final TopicPartition topicPartition) {
            return function.applyAsLong(topicPartition.partition());
        }
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
     *        resource allocation until first call to {@link KafkaRecordConsumer#consume(List)} or
     *        {@link KafkaRecordConsumer#acceptFailure(Throwable)} if appropriate.
     * @param partitionToInitialSeekOffset A function implementing a mapping from partition to its initial seek offset,
     *        or -1 if seek to beginning is intended.
     */
    @SuppressWarnings("rawtypes")
    public KafkaIngester(
            @NotNull final Logger log,
            @NotNull final Properties props,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final Function<TopicPartition, KafkaRecordConsumer> partitionToStreamConsumer,
            @NotNull final KafkaIngester.InitialOffsetLookup partitionToInitialSeekOffset,
            @Nullable final ConsumerLoopCallback consumerLoopCallback) {
        this.log = log;
        this.topic = topic;
        partitionDescription = partitionFilter.toString();
        logPrefix = KafkaIngester.class.getSimpleName() + "(" + topic + ", " + partitionDescription + "): ";
        kafkaConsumer = new KafkaConsumer(props);
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
            consumerLoopCallback.beforePoll(kafkaConsumer);
            final boolean more = pollOnce(Duration.ofNanos(remainingNanos));
            consumerLoopCallback.afterPoll(kafkaConsumer, more);
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
        try {
            ++pollCalls;
            records = kafkaConsumer.poll(timeout);
        } catch (WakeupException we) {
            // we interpret a wakeup as a signal to stop /this/ poll.
            return true;
        } catch (Exception ex) {
            log.error().append(logPrefix).append("Exception while polling for Kafka messages:").append(ex)
                    .append(", aborting.").endl();
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
                bytesProcessed += streamConsumer.consume(partitionRecords);
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
