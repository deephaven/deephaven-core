package io.deephaven.kafka.publish;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.table.impl.*;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is an internal implementation detail for io.deephaven.kafka; is not intended to be used directly by client
 * code. It lives in a separate package as a means of code organization.
 *
 */
public class PublishToKafka<K, V> extends LivenessArtifact {

    public static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerForClassWithDefault(PublishToKafka.class, "chunkSize", 2048);

    private final Table table;
    private final KafkaProducer<K, V> producer;
    private final String topic;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    @ReferentialIntegrity
    private final PublishListener publishListener;

    /**
     * <p>
     * Construct a publisher for {@code table} according the to Kafka {@code props} for the supplied {@code topic}.
     * <p>
     * The new publisher will produce records for existing {@code table} data at construction.
     * <p>
     * If {@code table} is a dynamic, refreshing table ({@link Table#isRefreshing()}), the calling thread must block the
     * {@link UpdateGraphProcessor#DEFAULT UpdateGraphProcessor} by holding either its
     * {@link UpdateGraphProcessor#exclusiveLock() exclusive lock} or its {@link UpdateGraphProcessor#sharedLock()
     * shared lock}. The publisher will install a listener in order to produce new records as updates become available.
     * Callers must be sure to maintain a reference to the publisher and ensure that it remains
     * {@link io.deephaven.engine.liveness.LivenessReferent live}. The easiest way to do this may be to construct the
     * publisher enclosed by a {@link io.deephaven.engine.liveness.LivenessScope liveness scope} with
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
     * @param topic The destination topic
     * @param keyColumns Optional array of string column names from table for the columns corresponding to Kafka's Key
     *        field.
     * @param keySerializer Optional {@link KeyOrValueSerializer} to produce Kafka record keys
     * @param valueColumns Optional array of string column names from table for the columns corresponding to Kafka's
     *        Value field.
     * @param valueSerializer Optional {@link KeyOrValueSerializer} to produce Kafka record values
     */
    public PublishToKafka(
            final Properties props,
            final Table table,
            final String topic,
            final String[] keyColumns,
            final KeyOrValueSerializer<K> keySerializer,
            final String[] valueColumns,
            final KeyOrValueSerializer<V> valueSerializer) {

        this.table = table;
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        // Publish the initial table state
        try (final PublicationGuard guard = new PublicationGuard()) {
            publishMessages(table.getRowSet(), false, true, guard);
        }

        // Install a listener to publish subsequent updates
        if (table.isRefreshing()) {
            table.listenForUpdates(publishListener = new PublishListener(
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

    private void publishMessages(@NotNull final RowSet rowsToPublish, final boolean usePrevious,
            final boolean publishValues, @NotNull final Callback callback) {
        if (rowsToPublish.isEmpty()) {
            return;
        }

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowsToPublish.size());
        try (final RowSequence.Iterator rowsIterator = rowsToPublish.getRowSequenceIterator();
                final KeyOrValueSerializer.Context keyContext =
                        keySerializer != null ? keySerializer.makeContext(chunkSize) : null;
                final KeyOrValueSerializer.Context valueContext =
                        publishValues && valueSerializer != null ? valueSerializer.makeContext(chunkSize) : null) {
            while (rowsIterator.hasMore()) {
                final RowSequence chunkRowKeys = rowsIterator.getNextRowSequenceWithLength(chunkSize);

                final ObjectChunk<K, Values> keyChunk;
                if (keyContext != null) {
                    keyChunk = keySerializer.handleChunk(keyContext, chunkRowKeys, usePrevious);
                } else {
                    keyChunk = null;
                }

                final ObjectChunk<V, Values> valueChunk;
                if (valueContext != null) {
                    valueChunk = valueSerializer.handleChunk(valueContext, chunkRowKeys, usePrevious);
                } else {
                    valueChunk = null;
                }

                for (int ii = 0; ii < chunkRowKeys.intSize(); ++ii) {
                    final ProducerRecord<K, V> record = new ProducerRecord<>(topic,
                            keyChunk != null ? keyChunk.get(ii) : null, valueChunk != null ? valueChunk.get(ii) : null);
                    producer.send(record, callback);
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

        private long sentCount;
        private final AtomicLong completedCount = new AtomicLong();
        private final AtomicReference<Exception> sendException = new AtomicReference<>();

        private void reset() {
            sentCount = 0;
            completedCount.set(0);
            sendException.set(null);
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
            try {
                if (sentCount == 0) {
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
                if (sentCount != localCompletedCount) {
                    throw new KafkaPublisherException(String.format("Sent count %d does not match completed count %d",
                            sentCount, localCompletedCount));
                }
            } finally {
                reset();
            }
        }
    }

    private class PublishListener extends InstrumentedTableUpdateListenerAdapter {

        private final ModifiedColumnSet keysModified;
        private final ModifiedColumnSet valuesModified;
        private final boolean isStream;

        private final PublicationGuard guard = new PublicationGuard();

        private PublishListener(
                @NotNull final ModifiedColumnSet keysModified,
                @NotNull final ModifiedColumnSet valuesModified) {
            super("PublishToKafka", table, false);
            this.keysModified = keysModified;
            this.valuesModified = valuesModified;
            this.isStream = StreamTableTools.isStream(table);
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            if (keySerializer != null || isStream) {
                Assert.assertion(upstream.shifted().empty(), "upstream.shifted.empty()");
            }
            Assert.assertion(!keysModified.containsAny(upstream.modifiedColumnSet()),
                    "!keysModified.containsAny(upstream.modifiedColumnSet())", "Key columns should never be modified");

            try (final SafeCloseable ignored = guard) {
                if (isStream) {
                    Assert.assertion(upstream.modified().isEmpty(), "upstream.modified.empty()");
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

    @Override
    protected void destroy() {
        super.destroy();
        producer.close();
    }
}
