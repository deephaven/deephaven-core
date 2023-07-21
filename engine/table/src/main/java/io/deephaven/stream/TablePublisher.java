package io.deephaven.stream;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateGraph;

import java.util.Objects;

public class TablePublisher {

    /**
     * Constructs a table publisher.
     *
     * <p>
     * Equivalent to calling {@link #of(String, TableDefinition, UpdateGraph, int)} with the {@code updateGraph} from
     * {@link ExecutionContext#getContext()} and {@code chunkSize} {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     *
     * @param name the name
     * @param definition the table definition
     * @return the table publisher
     */
    public static TablePublisher of(String name, TableDefinition definition) {
        return of(name, definition, ExecutionContext.getContext().getUpdateGraph(), ArrayBackedColumnSource.BLOCK_SIZE);
    }

    /**
     * Constructs a table publisher.
     *
     * @param name the name
     * @param definition the table definition
     * @param updateGraph the update graph
     * @param chunkSize the chunk size
     * @return the table publisher
     */
    public static TablePublisher of(String name, TableDefinition definition, UpdateGraph updateGraph, int chunkSize) {
        final TablePublisherImpl publisher = new TablePublisherImpl(name, definition, chunkSize);
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(definition, publisher, updateGraph, name);
        return new TablePublisher(publisher, adapter);
    }

    private final TablePublisherImpl publisher;
    // Eventually, we can close the StreamToBlinkTableAdapter
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blinkTable;

    private TablePublisher(TablePublisherImpl publisher, StreamToBlinkTableAdapter adapter) {
        this.publisher = Objects.requireNonNull(publisher);
        this.adapter = Objects.requireNonNull(adapter);
        this.blinkTable = adapter.table();
    }

    public Table blinkTable() {
        return blinkTable;
    }

    public void add(Table table) {
        publisher.add(table);
    }

    public void acceptFailure(Throwable e) {
        publisher.acceptFailure(e);
    }
}
