package io.deephaven.kafka.publish;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListenerAdapter;
import io.deephaven.db.v2.StreamTableTools;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PublishToKafka<K, V> {
    static final int CHUNK_SIZE = 2048;

    @NotNull
    private final Table table;
    private final KafkaProducer<K, V> producer;
    private final String topic;
    private final KeyOrValueSerializer<K> keySerializer;
    private final KeyOrValueSerializer<V> valueSerializer;

    @ReferentialIntegrity
    private final PublishListener publishListener;

    public PublishToKafka(final Properties props,
            final Table table,
            final String topic,
            final KeyOrValueSerializer<K> keySerializer,
            final KeyOrValueSerializer<V> valueSerializer) {
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;


        if (table.isLive()) {
            final DynamicTable dynTable = ((DynamicTable) table.coalesce());
            this.table = dynTable;
            dynTable.listenForUpdates(publishListener = new PublishListener());
        } else {
            this.table = table;
            publishListener = null;
        }

        // Publish the initial table state
        publishMessages(table.getIndex(), false, true);
    }

    public void shutdown() {
        if (!table.isLive()) {
            return;
        }
        final DynamicTable dynTable = (DynamicTable) table;
        dynTable.removeUpdateListener(publishListener);
    }

    private void publishMessages(ReadOnlyIndex indexToPublish, boolean usePrevious, boolean publishValues) {
        if (indexToPublish.isEmpty()) {
            return;
        }

        final int chunkSize = (int) Math.min(CHUNK_SIZE, indexToPublish.size());
        try (final OrderedKeys.Iterator okit = indexToPublish.getOrderedKeysIterator();
                final KeyOrValueSerializer.Context keyContext =
                        keySerializer != null ? keySerializer.makeContext(chunkSize) : null;
                final KeyOrValueSerializer.Context valueContext =
                        publishValues && valueSerializer != null ? valueSerializer.makeContext(chunkSize) : null;
                final WritableObjectChunk<Future<RecordMetadata>, Attributes.Values> sendFutures =
                        WritableObjectChunk.makeWritableChunk(chunkSize)) {
            while (okit.hasMore()) {
                final OrderedKeys chunkOk = okit.getNextOrderedKeysWithLength(chunkSize);

                final ObjectChunk<K, Attributes.Values> keyChunk;
                if (keyContext != null) {
                    keyChunk = keySerializer.handleChunk(keyContext, chunkOk, usePrevious);
                } else {
                    keyChunk = null;
                }

                final ObjectChunk<V, Attributes.Values> valueChunk;
                if (valueContext != null) {
                    valueChunk = valueSerializer.handleChunk(valueContext, chunkOk, usePrevious);
                } else {
                    valueChunk = null;
                }

                sendFutures.setSize(0);
                for (int ii = 0; ii < chunkOk.intSize(); ++ii) {
                    final ProducerRecord<K, V> record = new ProducerRecord<>(topic,
                            keyChunk != null ? keyChunk.get(ii) : null, valueChunk != null ? valueChunk.get(ii) : null);
                    Future<RecordMetadata> x = producer.send(record);
                    sendFutures.add(x);
                }

                // TODO: this makes us jerks for doing this midstream instead of waiting all the way until the end, but
                // we shouldn't just forget about them either; maybe we need to keep a few chunks of futures around, or
                // even we should be willing to have the whole thing be futured
                for (int ii = 0; ii < chunkOk.intSize(); ++ii) {
                    try {
                        sendFutures.get(ii).get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted while waiting for KafkaProducer send result",
                                e.getCause());
                    } catch (ExecutionException e) {
                        throw new RuntimeException("Failure while sending to KafkaProducer", e.getCause());
                    }
                }
            }
        }
    }

    private class PublishListener extends InstrumentedShiftAwareListenerAdapter {
        private final boolean isStream;

        private PublishListener() {
            super("PublishToKafka", (DynamicTable) table, false);
            this.isStream = StreamTableTools.isStream(table);
        }

        @Override
        public void onUpdate(Update upstream) {
            Assert.assertion(upstream.shifted.empty(), "upstream.shifted.empty()");

            if (isStream) {
                Assert.assertion(upstream.modified.empty(), "upstream.modified.empty()");
                publishMessages(upstream.added, false, true);
            } else {
                publishMessages(upstream.removed, true, false);
                try (final Index addedAndModified = upstream.added.union(upstream.modified)) {
                    publishMessages(addedAndModified, false, true);
                }
            }
        }
    }
}
