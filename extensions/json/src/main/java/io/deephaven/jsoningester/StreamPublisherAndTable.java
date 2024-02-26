package io.deephaven.jsoningester;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.util.List;

/**
 * Wrapper for a {@link #getPublisher() stream publisher}, a corresponding
 * {@link #getAdapter()  StreamToBlinkTableAdapter}, and the
 * {@link #getBlinkTable() blink table} produced by the adapter.
 */
public class StreamPublisherAndTable implements AutoCloseable {
    private final SimpleStreamPublisher publisher;
    private final StreamToBlinkTableAdapter adapter;

    public StreamPublisherAndTable(SimpleStreamPublisher publisher, StreamToBlinkTableAdapter adapter) {
        this.publisher = publisher;
        this.adapter = adapter;
    }

    public SimpleStreamPublisher getPublisher() {
        return publisher;
    }

    public StreamToBlinkTableAdapter getAdapter() {
        return adapter;
    }

    /**
     * Returns the adapter's blink table (see {@link StreamToBlinkTableAdapter#table()}).
     * @return The table produced by the {@link #getAdapter() StreamToBlinkTableAdapter}
     */
    public Table getBlinkTable() {
        return adapter.table();
    }

    @Override
    public void close() {
        adapter.close();
    }

    public static StreamPublisherAndTable createStreamPublisherAndTable(
            final List<String> colNames,
            final List<Class<?>> colTypes,
            final String adapterName) {
        return createStreamPublisherAndTable(colNames,
                colTypes,
                adapterName,
                ExecutionContext.getContext().getUpdateGraph());
    }

    public static StreamPublisherAndTable createStreamPublisherAndTable(
            final List<String> colNames,
            final List<Class<?>> colTypes,
            final String adapterName,
            final UpdateGraph updateSourceRegistrar) {
        final TableDefinition tableDef = TableDefinition.of(colNames, colTypes);
        return createStreamPublisherAndTable(tableDef, adapterName, updateSourceRegistrar);
    }

    public static StreamPublisherAndTable createStreamPublisherAndTable(
            final TableDefinition tableDefinition,
            final String adapterName,
            final UpdateSourceRegistrar updateSourceRegistrar) {
        final SimpleStreamPublisher publisher = new SimpleStreamPublisher(tableDefinition);

        final StreamToBlinkTableAdapter streamToBlinkTableAdapter = new StreamToBlinkTableAdapter(
                publisher.getTableDefinition(),
                publisher,
                updateSourceRegistrar,
                adapterName);

        return new StreamPublisherAndTable(publisher, streamToBlinkTableAdapter);
    }
}
