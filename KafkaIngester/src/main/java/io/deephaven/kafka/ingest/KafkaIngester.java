package io.deephaven.kafka.ingest;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.utils.DBTimeUtils;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.UnaryOperator;

/**
 * An ingester that replicates a Apache Kafka topic to a Deephaven Table
 *
 * <p>Each KafkaIngester is assigned a topic and a subset of Kafka partitions.  Each Kafka
 * partition is mapped to a Deephaven internal partition.  The column partition can be set through the constructor,
 * or defaults to {@link DBTimeUtils#currentDateNy()}.</p>
 *
 * <p>Automatic partition assignment and rebalancing are not supported. Each Kafka ingester instance must uniquely control its
 * checkpoint record, which is incompatible with rebalancing.</p>
 *
 * 
 */
public class KafkaIngester {
    private static final int REPORT_INTERVAL_MS = Configuration.getInstance().getIntegerWithDefault("KafkaIngester.reportIntervalMs", 60_000);
    private final KafkaConsumer<?, ?> consumer;
    private final Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory;
    @NotNull
    private final Logger log;
    private final DataImportServer dataImportServer;
    private final String topic;
    private final String partitionDescription;
    private final String logPrefix;
    private long messagesProcessed = 0;
    private long lastProcessed = 0;
    private final TIntObjectMap<StreamContext> partitionToChannel;
    private final Set<TopicPartition> openPartitions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final UnaryOperator<String> resumeFrom;


    /**
     * Constant predicate that returns true for all partitions.  This is the default, each and every partition that
     * exists will be handled by the same ingester.  Because Kafka consumers are inherently single threaded, to
     * scale beyond what a single consumer can handle, you must create multiple consumers each with a subset of
     * partitions using {@link PartitionRange}, {@link PartitionRoundRobin}, {@link SinglePartition} or a custom
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
         * @param endInclusive   the last partition for an ingester
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
            return Integer.toString(startInclusive) + (startInclusive == endInclusive ?  "" : endInclusive);
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
         *                      index between zero (inclusive) and consumerCount (exclusive)
         * @param consumerCount the number of consumers that will ingest this topic
         */
        public PartitionRoundRobin(int consumerIndex, int consumerCount) {
            this.consumerIndex = Require.geqZero(Require.lt(consumerIndex, "consumerIndex", consumerCount, "consumerCount"), "consumerIndex");
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
     * Creates a Kafka ingester for all partitions of a given topic and a Deephaven column partition of {@link DBTimeUtils#currentDateNy()}.
     *
     * @param log              a log for output
     * @param dataImportServer the dataimport server instance to attach our stream processor to
     * @param props            the properties used to create the {@link KafkaConsumer}
     * @param namespace        the namespace of the table to replicate to
     * @param tableName        the name of the table to replicate to
     * @param topic            the topic to replicate
     * @param adapterFactory   a function from the {@link TableWriter} for an internal partition to a suitable
     *                         {@link ConsumerRecordToTableWriterAdapter} class that handles records produced by the
     *                         Kafka topic and transforms them into Deephaven rows.
     */
    public KafkaIngester(final Logger log, final DataImportServer dataImportServer, final Properties props,
                         final String namespace, final String tableName, final String topic,
                         Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory) {
        this(log, dataImportServer, props, namespace, tableName, topic, ALL_PARTITIONS, DBTimeUtils.currentDateNy(), adapterFactory);
    }

    /**
     * Creates a Kafka ingester for all partitions of a given topic.
     *
     * @param log              a log for output
     * @param dataImportServer the dataimport server instance to attach our stream processor to
     * @param props            the properties used to create the {@link KafkaConsumer}
     * @param namespace        the namespace of the table to replicate to
     * @param tableName        the name of the table to replicate to
     * @param topic            the topic to replicate
     * @param columnPartition  the column partition that we will replicate to
     * @param adapterFactory   a function from the {@link TableWriter} for an internal partition to a suitable
     *                         {@link ConsumerRecordToTableWriterAdapter} class that handles records produced by the
     *                         Kafka topic and transforms them into Deephaven rows.
     */
    public KafkaIngester(final Logger log, final DataImportServer dataImportServer, final Properties props,
                         final String namespace, final String tableName, final String topic, final String columnPartition,
                         Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory) {
        this(log, dataImportServer, props, namespace, tableName, topic, ALL_PARTITIONS, columnPartition, adapterFactory);
    }

    /**
     * Creates a Kafka ingester for a given topic with a Deephaven column partition of {@link DBTimeUtils#currentDateNy()}.
     *
     * @param log              a log for output
     * @param dataImportServer the dataimport server instance to attach our stream processor to
     * @param props            the properties used to create the {@link KafkaConsumer}
     * @param namespace        the namespace of the table to replicate to
     * @param tableName        the name of the table to replicate to
     * @param topic            the topic to replicate
     * @param partitionFilter  a predicate indicating which partitions we should replicate
     * @param adapterFactory   a function from the {@link TableWriter} for an internal partition to a suitable
     *                         {@link ConsumerRecordToTableWriterAdapter} class that handles records produced by the
     *                         Kafka topic and transforms them into Deephaven rows.
     */
    public KafkaIngester(final Logger log, final DataImportServer dataImportServer, final Properties props,
                         final String namespace, final String tableName, final String topic, IntPredicate partitionFilter,
                         Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory) {
        this(log, dataImportServer, props, namespace, tableName, topic, partitionFilter, DBTimeUtils.currentDateNy(), adapterFactory);
    }

    /**
     * Creates a Kafka ingester for the given topic.
     *
     * @param log              a log for output
     * @param dataImportServer the dataimport server instance to attach our stream processor to
     * @param props            the properties used to create the {@link KafkaConsumer}
     * @param namespace        the namespace of the table to replicate to
     * @param tableName        the name of the table to replicate to
     * @param topic            the topic to replicate
     * @param partitionFilter  a predicate indicating which partitions we should replicate
     * @param columnPartition  the column partition that we will replicate to
     * @param adapterFactory   a function from the {@link TableWriter} for an internal partition to a suitable
     *                         {@link ConsumerRecordToTableWriterAdapter} class that handles records produced by the
     *                         Kafka topic and transforms them into Deephaven rows.
     */
    public KafkaIngester(final Logger log, final DataImportServer dataImportServer, final Properties props,
                         final String namespace, final String tableName, final String topic, IntPredicate partitionFilter,
                         String columnPartition, Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory) {
        this(log, dataImportServer, props, namespace, tableName, topic, partitionFilter, columnPartition, adapterFactory, null);
    }

    /**
     * Creates a Kafka ingester for the given topic.
     *
     * @param log              a log for output
     * @param dataImportServer the dataimport server instance to attach our stream processor to
     * @param props            the properties used to create the {@link KafkaConsumer}
     * @param namespace        the namespace of the table to replicate to
     * @param tableName        the name of the table to replicate to
     * @param topic            the topic to replicate
     * @param partitionFilter  a predicate indicating which partitions we should replicate
     * @param columnPartition  the column partition that we will replicate to
     * @param adapterFactory   a function from the {@link TableWriter} for an internal partition to a suitable
     *                         {@link ConsumerRecordToTableWriterAdapter} class that handles records produced by the
     *                         Kafka topic and transforms them into Deephaven rows.
     * @param resumeFrom       Given a column partition value, determine the prior column partition that we should read
     *                         a checkpoint record to resume from if we do not have our own checkpoint record.
     */
    public KafkaIngester(final Logger log, final DataImportServer dataImportServer, final Properties props,
                         final String namespace, final String tableName, final String topic, IntPredicate partitionFilter,
                         String columnPartition, Function<TableWriter, ConsumerRecordToTableWriterAdapter> adapterFactory,
                         UnaryOperator<String> resumeFrom) {
        this.log = log;
        this.dataImportServer = dataImportServer;
        this.topic = topic;
        this.partitionDescription = partitionFilter.toString();
        this.logPrefix = "KafkaIngester(" + topic + ", " + partitionDescription + ":" + namespace + "." + tableName + "): ";
        consumer = new KafkaConsumer(props);
        this.adapterFactory = adapterFactory;
        this.resumeFrom = resumeFrom;

        final List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        partitions.stream().filter(pi -> partitionFilter.test(pi.partition())).map(pi -> new TopicPartition(topic, pi.partition())).forEach(openPartitions::add);

        consumer.assign(openPartitions);

        final Set<TopicPartition> assignments = consumer.assignment();
        log.info().append(logPrefix).append("Partition Assignments: ").append(assignments.toString()).endl();

        if (assignments.size() != openPartitions.size()) {
            throw new RuntimeException(logPrefix +  "Partition assignments do not match request: assignments=" + assignments + ", request=" + openPartitions);
        }

        partitionToChannel = new TIntObjectHashMap<>(partitions.size());

        for (final TopicPartition topicPartition : assignments) {
            consumer.seekToBeginning(Collections.singletonList(topicPartition));
            final int partition = topicPartition.partition();
            final String internalPartition = topicPartition.topic() + "-" + partition;
            final FullTableLocationKey streamKey = new FullTableLocationKey(namespace, tableName, TableType.SYSTEM_INTRADAY, internalPartition, columnPartition);
            final StreamContext streamContext = new StreamContext(topicPartition);

            final SimpleDataImportStreamProcessor processor = dataImportServer.createSimpleProcessor("Kafka: " + streamKey, streamKey, streamContext::close, streamContext::onReject);
            final SimpleDataImportStreamProcessor.Context context = processor.initialize();
            if (context == null) {
                if (streamContext.rejectMessage != null) {
                    throw new RuntimeException(logPrefix + "Could not initialize stream for " + topicPartition + ": " + streamContext.rejectMessage, streamContext.rejectException);
                } else {
                    throw new RuntimeException(logPrefix + "Could not initialize stream for " + topicPartition);
                }
            }
            processor.processData(() -> streamContext.setProcessorAndContext(processor, context));

            partitionToChannel.put(partition, streamContext);
        }
    }

    /**
     * Starts a consumer thread which replicates the consumed Kafka messages to Deephaven.
     * <p>
     * This method must not be called more than once on an ingester instance.
     */
    public void start() {
        final Thread t = new Thread(this::consumerLoop, "KafkaIngester-" + topic + ":" + partitionDescription);
        t.setDaemon(true);
        t.start();
    }

    private void consumerLoop() {
        final long reportIntervalNanos = REPORT_INTERVAL_MS * 1_000_000L;
        long lastReportNanos = System.nanoTime();
        final int expectedPartitionCount = openPartitions.size();
        while (openPartitions.size() == expectedPartitionCount) {
            final long beforePoll = System.nanoTime();
            final long nextReport = lastReportNanos + reportIntervalNanos;
            final long remainingNanos = beforePoll > nextReport ? 0 : (nextReport - beforePoll);
            pollOnce(Duration.ofNanos(remainingNanos));
            final long afterPoll = System.nanoTime();
            if (afterPoll > nextReport) {
                final long intervalMessages = messagesProcessed - lastProcessed;
                final long intervalNanos = afterPoll - lastReportNanos;
                log.info().append(logPrefix).append("Processed ").append(intervalMessages).append(" in ").append(intervalNanos / 1000_000L).append("ms, ").appendDouble(1000000000.0 * intervalMessages / intervalNanos, 4, false, false).append(" msgs/sec").endl();
                lastReportNanos = afterPoll;
                lastProcessed = messagesProcessed;
            }
        }
        log.info().append(logPrefix).append("Closing Kafka consumer.").endl();
        consumer.close();
        ProcessEnvironment.getGlobalFatalErrorReporter().report(logPrefix + "One or more Kafka streams was closed, terminating process.");
    }

    private void pollOnce(Duration timeout) {
        final ConsumerRecords<?, ?> records;
        try {
            records = consumer.poll(timeout);
        } catch (WakeupException we) {
            return;
        }
        for (final ConsumerRecord<?, ?> record : records) {
            final int partition = record.partition();
            final StreamContext streamContext = partitionToChannel.get(partition);
            if (streamContext == null) {
                log.warn().append(logPrefix).append("No stream context found for partition ").append(partition).endl();
                continue;
            }
            streamContext.handleRecord(record);
            ++messagesProcessed;
        }
    }

    private class StreamContext implements CheckpointRecord.SourceFileSizeRecord {
        private final TopicPartition topicPartition;
        private SimpleDataImportStreamProcessor processor;
        private ConsumerRecordToTableWriterAdapter adapter;
        private long nextOffset;
        private volatile boolean closed = false;
        private String rejectMessage;
        private Exception rejectException;

        private StreamContext(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        void setProcessorAndContext(SimpleDataImportStreamProcessor processor, SimpleDataImportStreamProcessor.Context context) {
            this.processor = Require.neqNull(processor, "processor");
            Require.neqNull(context, "context");

            final IWritableLocalTableLocation location = context.getLocation();
            final CheckpointRecord writerCheckpointRecord = location.getWriterCheckpointRecord();

            final CheckpointRecord.SourceFileSizeRecord sourceFileSizeRecord = writerCheckpointRecord.getSourceFileSizeRecord();
            if (sourceFileSizeRecord == null) {
                if (resumeFrom != null) {
                    final FullTableLocationKey streamKey = context.getStreamKey();
                    final String priorColumnPartition = resumeFrom.apply(streamKey.getColumnPartition());
                    final FullTableLocationKey priorTableKey = new FullTableLocationKey(streamKey.getNamespace(), streamKey.getTableName(), streamKey.getTableType(), streamKey.getInternalPartition(), priorColumnPartition);
                    log.info().append(logPrefix).append("Computed prior table location for resuming Kafka stream: ").append(priorTableKey).endl();
                    final WritableLocalTableLocationProvider tableLocationProvider = (WritableLocalTableLocationProvider) dataImportServer.getTableDataService().getTableLocationProvider(priorTableKey.getTableKey());
                    tableLocationProvider.refresh();
                    final WritableLocalTableLocation priorTableLocation = (WritableLocalTableLocation) tableLocationProvider.getTableLocationIfPresent(priorTableKey.getTableLocationKey());
                    CheckpointRecord.SourceFileSizeRecord priorSourceRecord = null;

                    final File checkpointRecordFile;
                    if (priorTableLocation != null) {
                        checkpointRecordFile = priorTableLocation.getCheckpointRecordFile();
                        final CheckpointRecord priorCheckpoint = WritableLocalTableLocation.readCheckpointRecordForImportersFromFile(checkpointRecordFile);
                        priorSourceRecord = priorCheckpoint.getSourceFileSizeRecord();
                    } else {
                        checkpointRecordFile = null;
                    }

                    if (priorSourceRecord != null) {
                        final String topicName = priorSourceRecord.getName();
                        if (!topicName.equals(topicPartition.toString())) {
                            throw new IllegalStateException(logPrefix + "Topic partition mismatch: " + topicName + " read from prior checkpoint record at " + checkpointRecordFile +  " does not match " + topicPartition.toString());
                        }
                        final long seekOffset = priorSourceRecord.getSize();
                        log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to offset ").append(seekOffset).append(" based on prior checkpoint record at ").append(checkpointRecordFile.toString()).endl();
                        consumer.seek(topicPartition, seekOffset);
                    } else {
                        log.info().append(logPrefix).append(location).append(" has no prior checkpoint record at ").append(Objects.toString(checkpointRecordFile)).append(", seeking to beginning of ").append(topicPartition.toString()).endl();
                        consumer.seekToBeginning(Collections.singletonList(topicPartition));
                    }
                } else {
                    log.info().append(logPrefix).append(location).append(" has a null source file size record, seeking to beginning of ").append(topicPartition.toString()).endl();
                    consumer.seekToBeginning(Collections.singletonList(topicPartition));
                }
            } else {
                final String topicName = sourceFileSizeRecord.getName();
                if (!topicName.equals(topicPartition.toString())) {
                    throw new IllegalStateException(logPrefix + "Topic partition mismatch: " + topicName + " read from checkpoint record does not match " + topicPartition.toString());
                }
                final long seekOffset = sourceFileSizeRecord.getSize();
                log.info().append(logPrefix).append(topicPartition.toString()).append(" seeking to offset ").append(seekOffset).endl();
                consumer.seek(topicPartition, seekOffset);
            }
            nextOffset = consumer.position(topicPartition);
            log.info().append(logPrefix).append(topicPartition.toString()).append(" processing beginning at offset ").append(nextOffset).endl();

            writerCheckpointRecord.setSourceFileSizeRecord(this);

            adapter = adapterFactory.apply(context.getTableWriter());
        }

        void handleRecord(ConsumerRecord<?, ?> record) {
            if (closed) {
                return;
            }
            processor.processData(() -> {
                try {
                    adapter.consumeRecord(record);
                } catch (Exception e) {
                    log.error().append(logPrefix).append("Failed to consume Kafka record from ").append(topicPartition.toString()).append(": ").append(e).endl();
                    throw new KafkaIngesterException("Failed to consume Kafka record: " + record, e);
                }
                nextOffset = consumer.position(topicPartition);
            });
        }

        void close() {
            log.info().append(logPrefix).append(" closing ").append(topicPartition.toString()).endl();
            closed = true;
            openPartitions.remove(topicPartition);
            consumer.wakeup();
        }

        void onReject(String message, Exception cause) {
            rejectMessage = message;
            rejectException = cause;
        }

        @Override
        public String getName() {
            return topicPartition.toString();
        }

        @Override
        public long getSize() {
            return nextOffset;
        }
    }
}
