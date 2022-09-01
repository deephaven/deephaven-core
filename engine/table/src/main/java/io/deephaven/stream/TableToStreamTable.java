package io.deephaven.stream;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.util.SafeCloseable;

import javax.annotation.Nullable;
import java.util.Objects;


/**
 * A {@link Table Table-based} consumption model that abstracts some of the implementation details of
 * {@link StreamToTableAdapter} and makes it easy to consume data via {@link TableStreamConsumer}.
 */
public final class TableToStreamTable implements Runnable, SafeCloseable {

    public static TableToStreamTable of(
            String name,
            TableDefinition definition,
            UpdateSourceRegistrar updateSourceRegistrar,
            @Nullable Runnable shutdownCallback) {
        final SetableStreamPublisher publisher = new SetableStreamPublisher();
        final StreamToTableAdapter adapter =
                new StreamToTableAdapter(definition, publisher, updateSourceRegistrar, name);
        if (shutdownCallback != null) {
            adapter.setShutdownCallback(shutdownCallback);
        }
        return new TableToStreamTable(publisher.consumer(), adapter, publisher);
    }

    private final StreamConsumer consumer;
    private final StreamToTableAdapter adapter;
    private final SetableStreamPublisher publisher;

    private TableToStreamTable(StreamConsumer consumer, StreamToTableAdapter adapter,
            SetableStreamPublisher publisher) {
        this.consumer = Objects.requireNonNull(consumer);
        this.adapter = Objects.requireNonNull(adapter);
        this.publisher = Objects.requireNonNull(publisher);
    }

    public void setFlushDelegate(Runnable flushDelegate) {
        publisher.setFlushDelegate(flushDelegate);
    }

    /**
     * Return the {@link Table#STREAM_TABLE_ATTRIBUTE stream} {@link Table table} that this adapter is producing, and
     * ensure that {@code this} no longer enforces strong reachability of the result. May return {@code null} if invoked
     * more than once.
     *
     * @return The resulting stream table
     */
    public Table table() {
        return adapter.table();
    }

    /**
     * Create a table stream consumer.
     *
     * @param chunkSize the chunk size
     * @param sync whether chunked additions to the consumer need to be synchronous
     * @return the consumer
     */
    public TableStreamConsumer consumer(int chunkSize, boolean sync) {
        return new TableStreamConsumer(consumer, adapter.tableDefinition(), chunkSize, sync);
    }

    @Override
    public void run() {
        adapter.run();
    }

    @Override
    public void close() {
        adapter.close();
    }
}
