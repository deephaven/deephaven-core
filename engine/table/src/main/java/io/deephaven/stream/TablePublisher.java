package io.deephaven.stream;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.annotations.TestUseOnly;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Produces a {@link Table#BLINK_TABLE_ATTRIBUTE blink table} from {@link #add(Table) added tables}.
 */
public class TablePublisher {

    /**
     * Constructs a table publisher.
     *
     * <p>
     * The {@code onShutdownCallback}, if present, is called one time when the publisher should stop publishing new data
     * and release any related resources as soon as practicable since publishing won't have any downstream effects.
     *
     * <p>
     * Equivalent to calling {@link #of(String, TableDefinition, Runnable, UpdateGraph, int)} with the
     * {@code updateGraph} from {@link ExecutionContext#getContext()} and {@code chunkSize}
     * {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     *
     * @param name the name
     * @param definition the table definition
     * @param onShutdownCallback the on-shutdown callback
     * @return the table publisher
     */
    public static TablePublisher of(String name, TableDefinition definition, @Nullable Runnable onShutdownCallback) {
        return of(name, definition, onShutdownCallback, ExecutionContext.getContext().getUpdateGraph(),
                ArrayBackedColumnSource.BLOCK_SIZE);
    }

    /**
     * Constructs a table publisher.
     *
     * <p>
     * The {@code onShutdownCallback}, if present, is called one time when the publisher should stop publishing new data
     * and release any related resources as soon as practicable since publishing won't have any downstream effects.
     *
     * <p>
     * The {@code chunkSize} is the size at which chunks will be filled from the source table during an
     * {@link TablePublisher#add}. The suggested value is {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     *
     * @param name the name
     * @param definition the table definition
     * @param onShutdownCallback the on-shutdown callback
     * @param updateGraph the update graph for the blink table
     * @param chunkSize the chunk size is the maximum size
     * @return the table publisher
     */
    public static TablePublisher of(String name, TableDefinition definition, @Nullable Runnable onShutdownCallback,
            UpdateGraph updateGraph, int chunkSize) {
        final TableStreamPublisherImpl publisher =
                new TableStreamPublisherImpl(name, definition, onShutdownCallback, chunkSize);
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(definition, publisher, updateGraph, name);
        return new TablePublisher(publisher, adapter);
    }

    private final TableStreamPublisherImpl publisher;
    private final StreamToBlinkTableAdapter adapter;

    private TablePublisher(TableStreamPublisherImpl publisher, StreamToBlinkTableAdapter adapter) {
        this.publisher = Objects.requireNonNull(publisher);
        this.adapter = Objects.requireNonNull(adapter);
    }

    /**
     * The {@link #table blink table's} definition.
     *
     * @return the definition
     */
    public TableDefinition definition() {
        return publisher.definition();
    }

    /**
     * The {@link Table#BLINK_TABLE_ATTRIBUTE blink table}.
     *
     * <p>
     * May return {@code null} if invoked more than once and the initial caller does not enforce strong reachability of
     * the result.
     *
     * @return the blink table
     */
    public Table table() {
        return adapter.table();
    }

    /**
     * Adds a snapshot of the data from {@code table} into the {@link #table blink table} according to the blink table's
     * {@link #definition() definition}.
     *
     * <p>
     * All of the data from {@code table} will be:
     *
     * <ol>
     * <li><b>consistent</b> with a point in time</li>
     * <li><b>fully contained</b> in a single blink table's update cycle</li>
     * <li><b>non-interleaved</b> with any other calls to add (concurrent, or not)</li>
     * </ol>
     *
     * @param table the table to add
     */
    public void add(Table table) {
        publisher.add(table);
    }

    /**
     * Publish a {@code failure} for notification to the {@link io.deephaven.engine.table.TableListener listeners} of
     * the {@link #table() blink table}. Future calls to {@link #add(Table)} will silently return. Will cause the
     * on-shutdown callback to be invoked if it hasn't already been invoked.
     *
     * @param failure the failure
     */
    public void publishFailure(Throwable failure) {
        publisher.publishFailure(failure);
    }

    /**
     * Checks whether {@code this} is alive; if {@code false}, the publisher should stop publishing new data and release
     * any related resources as soon as practicable since publishing won't have any downstream effects.
     *
     * <p>
     * Once this is {@code false}, it will always remain {@code false}. For more prompt notifications, publishers may
     * prefer to use on-shutdown callbacks.
     *
     * @return if this is alive
     */
    public boolean isAlive() {
        return adapter.isAlive();
    }

    @TestUseOnly
    void runForUnitTests() {
        adapter.run();
    }
}
