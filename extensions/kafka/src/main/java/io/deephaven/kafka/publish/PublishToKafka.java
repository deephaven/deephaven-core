//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.publish;

import io.deephaven.api.ColumnName;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.kafka.KafkaPublishOptions;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is an internal implementation detail for io.deephaven.kafka; is not intended to be used directly by client
 * code. It lives in a separate package as a means of code organization.
 */
@InternalUseOnly
public class PublishToKafka<K, V> extends LivenessArtifact {

    public static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerForClassWithDefault(PublishToKafka.class, "chunkSize", 2048);

    private final Table table;
    private final KafkaProducer<K, V> producer;
    private final String defaultTopic;
    private final Integer defaultPartition;
    private final KeyOrValueSerializer<K> keyChunkSerializer;
    private final KeyOrValueSerializer<V> valueChunkSerializer;
    private final ColumnSource<CharSequence> topicColumnSource;
    private final ColumnSource<Integer> partitionColumnSource;
    private final ColumnSource<Long> timestampColumnSource;

    @ReferentialIntegrity
    private final PublishListener publishListener;

    /**
     * @deprecated please use {@link io.deephaven.kafka.KafkaTools#produceFromTable(KafkaPublishOptions)}
     */
    @Deprecated(forRemoval = true)
    public PublishToKafka(
            final Properties props,
            final Table table,
            final String topic,
            final String[] keyColumns,
            final Serializer<K> kafkaKeySerializer,
            final KeyOrValueSerializer<K> keyChunkSerializer,
            final String[] valueColumns,
            final Serializer<V> kafkaValueSerializer,
            final KeyOrValueSerializer<V> valueChunkSerializer,
            final boolean publishInitial) {
        this(props, table, topic, null, keyColumns, kafkaKeySerializer, keyChunkSerializer, valueColumns,
                kafkaValueSerializer, valueChunkSerializer, null, null, null, publishInitial);
    }

    /**
     * <p>
     * Construct a publisher for {@code table} according the to Kafka {@code props} for the supplied {@code topic}.
     * <p>
     * The new publisher will produce records for existing {@code table} data at construction.
     * <p>
     * If {@code table} is a dynamic, refreshing table ({@link Table#isRefreshing()}), the calling thread must block the
     * {@link UpdateGraph update graph} by holding either its {@link UpdateGraph#exclusiveLock() exclusive lock} or its
     * {@link UpdateGraph#sharedLock() shared lock}. The publisher will install a listener in order to produce new
     * records as updates become available. Callers must be sure to maintain a reference to the publisher and ensure
     * that it remains {@link io.deephaven.engine.liveness.LivenessReferent live}. The easiest way to do this may be to
     * construct the publisher enclosed by a {@link io.deephaven.engine.liveness.LivenessScope liveness scope} with
     * {@code enforceStrongReachability} specified as {@code true}, and {@link LivenessScope#release() release} the
     * scope when publication is no longer needed. For example:
     *
     * <pre>
     *     // To initiate publication:
     *     final LivenessScope publisherScope = new LivenessScope(true);
     *     try (final SafeCloseable ignored = LivenessScopeStack.open(publisherScope, false)) {
     *         new PublishToKafka(...);
     *     }
     *     // To cease publication:
     *     publisherScope.release();
     * </pre>
     *
     * @param props The Kafka {@link Properties}
     * @param table The source {@link Table}
     * @param defaultTopic The default destination topic
     * @param defaultPartition The default destination partition
     * @param keyColumns Optional array of string column names from table for the columns corresponding to Kafka's Key
     *        field.
     * @param kafkaKeySerializer The kafka {@link Serializer} to use for keys
     * @param keyChunkSerializer Optional {@link KeyOrValueSerializer} to consume table data and produce Kafka record
     *        keys in chunk-oriented fashion
     * @param valueColumns Optional array of string column names from table for the columns corresponding to Kafka's
     *        Value field.
     * @param kafkaValueSerializer The kafka {@link Serializer} to use for values
     * @param valueChunkSerializer Optional {@link KeyOrValueSerializer} to consume table data and produce Kafka record
     *        values in chunk-oriented fashion
     * @param publishInitial If the initial data in {@code table} should be published
     * @param topicColumn The topic column. When set, uses the the given {@link CharSequence} column from {@code table}
     *        as the first source for setting the kafka record topic.
     * @param partitionColumn The partition column. When set, uses the the given {@code int} column from {@code table}
     *        as the first source for setting the kafka record partition.
     * @param timestampColumn The timestamp column. When set, uses the the given {@link Instant} column from
     *        {@code table} as the first source for setting the kafka record timestamp.
     */
    public PublishToKafka(
            final Properties props,
            Table table,
            final String defaultTopic,
            final Integer defaultPartition,
            final String[] keyColumns,
            final Serializer<K> kafkaKeySerializer,
            final KeyOrValueSerializer<K> keyChunkSerializer,
            final String[] valueColumns,
            final Serializer<V> kafkaValueSerializer,
            final KeyOrValueSerializer<V> valueChunkSerializer,
            final ColumnName topicColumn,
            final ColumnName partitionColumn,
            final ColumnName timestampColumn,
            final boolean publishInitial) {
        this.table = (table = table.coalesce());
        this.producer = new KafkaProducer<>(
                props,
                Objects.requireNonNull(kafkaKeySerializer),
                Objects.requireNonNull(kafkaValueSerializer));
        this.defaultTopic = defaultTopic;
        this.defaultPartition = defaultPartition;
        this.keyChunkSerializer = keyChunkSerializer;
        this.valueChunkSerializer = valueChunkSerializer;
        this.topicColumnSource = topicColumn == null
                ? null
                : table.getColumnSource(topicColumn.name(), CharSequence.class);
        this.partitionColumnSource = partitionColumn == null
                ? null
                : table.getColumnSource(partitionColumn.name(), int.class);
        this.timestampColumnSource = timestampColumn == null
                ? null
                : ReinterpretUtils.instantToLongSource(table.getColumnSource(timestampColumn.name(), Instant.class));
        if (publishInitial) {
            // Publish the initial table state
            try (final PublicationGuard guard = new PublicationGuard()) {
                publishMessages(table.getRowSet(), false, true, guard);
            }
        }
        // Install a listener to publish subsequent updates
        if (table.isRefreshing()) {
            table.addUpdateListener(publishListener = new PublishListener(
                    getModifiedColumnSet(table, keyColumns),
                    getModifiedColumnSet(table, valueColumns)));
            manage(publishListener);
        } else {
            publishListener = null;
            producer.close();
        }
    }

    private static ModifiedColumnSet getModifiedColumnSet(@NotNull final Table table, final String[] columns) {
        return (columns == null)
                ? ModifiedColumnSet.EMPTY
                : ((QueryTable) table).newModifiedColumnSet(columns);
    }

    private String topic(ObjectChunk<CharSequence, ?> topicChunk, int index) {
        if (topicChunk == null) {
            return defaultTopic;
        }
        final CharSequence charSequence = topicChunk.get(index);
        return charSequence == null ? defaultTopic : charSequence.toString();
    }

    private Integer partition(IntChunk<?> partitionChunk, int index) {
        if (partitionChunk == null) {
            return defaultPartition;
        }
        final int partition = partitionChunk.get(index);
        return partition == QueryConstants.NULL_INT ? defaultPartition : Integer.valueOf(partition);
    }

    public static Long timestampMillis(LongChunk<?> nanosChunk, int index) {
        if (nanosChunk == null) {
            return null;
        }
        final long nanos = nanosChunk.get(index);
        return nanos == QueryConstants.NULL_LONG ? null : TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    private static <T> T object(ObjectChunk<T, ?> chunk, int index) {
        return chunk == null ? null : chunk.get(index);
    }

    private static ChunkSource.GetContext makeGetContext(ColumnSource<?> source, int chunkSize) {
        return source == null ? null : source.makeGetContext(chunkSize);
    }

    private void publishMessages(@NotNull final RowSet rowsToPublish, final boolean usePrevious,
            final boolean publishValues, @NotNull final PublicationGuard guard) {
        if (rowsToPublish.isEmpty()) {
            return;
        }
        guard.onSend(rowsToPublish.size());

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowsToPublish.size());
        try (final RowSequence.Iterator rowsIterator = rowsToPublish.getRowSequenceIterator();
                final KeyOrValueSerializer.Context keyContext = keyChunkSerializer != null
                        ? keyChunkSerializer.makeContext(chunkSize)
                        : null;
                final KeyOrValueSerializer.Context valueContext = publishValues && valueChunkSerializer != null
                        ? valueChunkSerializer.makeContext(chunkSize)
                        : null;
                final ChunkSource.GetContext topicContext = makeGetContext(topicColumnSource, chunkSize);
                final ChunkSource.GetContext partitionContext = makeGetContext(partitionColumnSource, chunkSize);
                final ChunkSource.GetContext timestampContext = makeGetContext(timestampColumnSource, chunkSize)) {
            while (rowsIterator.hasMore()) {
                final RowSequence chunkRowKeys = rowsIterator.getNextRowSequenceWithLength(chunkSize);

                final ObjectChunk<K, ?> keyChunk = keyContext == null
                        ? null
                        : keyChunkSerializer.handleChunk(keyContext, chunkRowKeys, usePrevious);

                final ObjectChunk<V, ?> valueChunk = valueContext == null
                        ? null
                        : valueChunkSerializer.handleChunk(valueContext, chunkRowKeys, usePrevious);

                final ObjectChunk<CharSequence, ?> topicChunk = topicContext == null
                        ? null
                        : (usePrevious
                                ? topicColumnSource.getPrevChunk(topicContext, chunkRowKeys)
                                : topicColumnSource.getChunk(topicContext, chunkRowKeys))
                                .asObjectChunk();

                final IntChunk<?> partitionChunk = partitionContext == null
                        ? null
                        : (usePrevious
                                ? partitionColumnSource.getPrevChunk(partitionContext, chunkRowKeys)
                                : partitionColumnSource.getChunk(partitionContext, chunkRowKeys))
                                .asIntChunk();

                final LongChunk<?> timestampChunk = timestampContext == null
                        ? null
                        : (usePrevious
                                ? timestampColumnSource.getPrevChunk(timestampContext, chunkRowKeys)
                                : timestampColumnSource.getChunk(timestampContext, chunkRowKeys))
                                .asLongChunk();

                final int numRecords = chunkRowKeys.intSize();
                for (int ii = 0; ii < numRecords; ++ii) {
                    final ProducerRecord<K, V> record = new ProducerRecord<>(
                            topic(topicChunk, ii),
                            partition(partitionChunk, ii),
                            timestampMillis(timestampChunk, ii),
                            object(keyChunk, ii),
                            object(valueChunk, ii));
                    producer.send(record, guard);
                }
            }
        }
    }

    /**
     * Re-usable, {@link SafeCloseable} {@link Callback} used to bracket multiple calls to
     * {@link KafkaProducer#send(ProducerRecord, Callback) send} and ensure correct completion. Used in the following
     * pattern:
     * 
     * <pre>
     * final PublicationGuard guard = new PublicationGuard();
     * try (final Closeable ignored = guard) {
     *     // Call producer.send(record, guard) 0 or more times
     * }
     * </pre>
     */
    private class PublicationGuard implements Callback, SafeCloseable {

        private final AtomicLong sentCount = new AtomicLong();
        private final AtomicLong completedCount = new AtomicLong();
        private final AtomicReference<Exception> sendException = new AtomicReference<>();

        private volatile boolean closed;

        private void reset() {
            sentCount.set(0);
            completedCount.set(0);
            sendException.set(null);
            closed = false;
        }

        private void onSend(final long messagesToSend) {
            if (closed) {
                throw new IllegalStateException("Tried to send using a guard that is no longer open");
            }
            sentCount.addAndGet(messagesToSend);
        }

        @Override
        public void onCompletion(@NotNull final RecordMetadata metadata, final Exception exception) {
            completedCount.getAndIncrement();
            if (exception != null) {
                sendException.compareAndSet(null, exception);
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                final long localSentCount = sentCount.get();
                if (localSentCount == 0) {
                    return;
                }
                try {
                    producer.flush();
                } catch (Exception e) {
                    throw new KafkaPublisherException("KafkaProducer reported flush failure", e);
                }
                final Exception localSendException = sendException.get();
                if (localSendException != null) {
                    throw new KafkaPublisherException("KafkaProducer reported send failure", localSendException);
                }
                final long localCompletedCount = completedCount.get();
                if (localSentCount != localCompletedCount) {
                    throw new KafkaPublisherException(String.format("Sent count %d does not match completed count %d",
                            localSentCount, localCompletedCount));
                }
            } finally {
                reset();
            }
        }
    }

    private class PublishListener extends InstrumentedTableUpdateListenerAdapter {

        private final ModifiedColumnSet keysModified;
        private final ModifiedColumnSet valuesModified;
        private final boolean isBlink;

        private final PublicationGuard guard = new PublicationGuard();

        private PublishListener(
                @NotNull final ModifiedColumnSet keysModified,
                @NotNull final ModifiedColumnSet valuesModified) {
            super("PublishToKafka", table, false);
            this.keysModified = keysModified;
            this.valuesModified = valuesModified;
            this.isBlink = BlinkTableTools.isBlink(table);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            Assert.assertion(!keysModified.containsAny(upstream.modifiedColumnSet()),
                    "!keysModified.containsAny(upstream.modifiedColumnSet())", "Key columns should never be modified");

            try (final SafeCloseable ignored = guard) {
                if (isBlink) {
                    Assert.assertion(upstream.modified().isEmpty(), "upstream.modified.empty()");
                    Assert.assertion(upstream.shifted().empty(), "upstream.shifted.empty()");
                    // We always ignore removes on streams, and expect no modifies or shifts
                    publishMessages(upstream.added(), false, true, guard);
                    return;
                }

                // Regular table, either keyless, add-only, or aggregated
                publishMessages(upstream.removed(), true, false, guard);
                if (valuesModified.containsAny(upstream.modifiedColumnSet())) {
                    try (final RowSet addedAndModified = upstream.added().union(upstream.modified())) {
                        publishMessages(addedAndModified, false, true, guard);
                    }
                } else {
                    publishMessages(upstream.added(), false, true, guard);
                }
            }
        }
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        producer.close();
    }
}
