package io.deephaven.stream;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.annotations.TestUseOnly;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Produces a {@link Table#BLINK_TABLE_ATTRIBUTE blink table} from {@link #add(Table) added tables}.
 */
public class TablePublisher {

    /**
     * Constructs a table publisher.
     *
     * <p>
     * The {@code onFlushCallback}, if present, is called once before each update graph cycle to allow the publisher to
     * flush any outstanding data. This is a useful pattern to allow publishers to be efficient with batched data.
     *
     * <p>
     * The {@code onShutdownCallback}, if present, is called one time when the publisher should stop publishing new data
     * and release any related resources as soon as practicable since publishing won't have any downstream effects.
     *
     * <p>
     * Equivalent to calling {@link #of(String, TableDefinition, Consumer, Runnable, UpdateGraph, int)} with the
     * {@code updateGraph} from {@link ExecutionContext#getContext()} and {@code chunkSize}
     * {@value ArrayBackedColumnSource#BLOCK_SIZE}.
     *
     * @param name the name
     * @param definition the table definition
     * @param onFlushCallback the on-flush callback
     * @param onShutdownCallback the on-shutdown callback
     * @return the table publisher
     */
    public static TablePublisher of(
            String name,
            TableDefinition definition,
            @Nullable Consumer<TablePublisher> onFlushCallback,
            @Nullable Runnable onShutdownCallback) {
        return of(name, definition, onFlushCallback, onShutdownCallback, ExecutionContext.getContext().getUpdateGraph(),
                ArrayBackedColumnSource.BLOCK_SIZE);
    }

    /**
     * Constructs a table publisher.
     *
     * <p>
     * The {@code onFlushCallback}, if present, is called once before each update graph cycle to allow the publisher to
     * flush any outstanding data. This is a useful pattern to allow publishers to be efficient with batched data.
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
     * @param onFlushCallback the on-flush callback
     * @param onShutdownCallback the on-shutdown callback
     * @param updateGraph the update graph for the blink table
     * @param chunkSize the chunk size is the maximum size
     * @return the table publisher
     */
    public static TablePublisher of(
            String name,
            TableDefinition definition,
            @Nullable Consumer<TablePublisher> onFlushCallback,
            @Nullable Runnable onShutdownCallback,
            UpdateGraph updateGraph,
            int chunkSize) {
        final TablePublisher[] publisher = new TablePublisher[1];
        final TableStreamPublisherImpl impl =
                new TableStreamPublisherImpl(name, definition,
                        onFlushCallback == null ? null : () -> onFlushCallback.accept(publisher[0]), onShutdownCallback,
                        chunkSize);
        final StreamToBlinkTableAdapter adapter =
                new StreamToBlinkTableAdapter(definition, impl, updateGraph, name, Map.of(), false);
        publisher[0] = new TablePublisher(impl, adapter);
        adapter.initialize();
        return publisher[0];
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
     * Adds a snapshot of the data from {@code table} into the {@link #table blink table}. The added {@code table} must
     * contain a superset of the columns from the {@link #definition() definition}; the columns may be in any order.
     * Columns from {@code table} that are not in the {@link #definition() definition} are ignored.
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
     * Indicate that data publication has failed. {@link #table() Blink table}
     * {@link io.deephaven.engine.table.TableListener listeners} will be notified of the failure, the on-shutdown
     * callback will be invoked if it hasn't already been, {@code this} publisher will no longer be {@link #isAlive()
     * alive}, and future calls to {@link #add(Table) add} will silently return without publishing. These effects may
     * resolve asynchronously.
     *
     * @param failure the failure
     */
    public void publishFailure(Throwable failure) {
        publisher.publishFailure(failure);
    }

    /**
     * Checks whether {@code this} is alive; if {@code false}, the caller should stop adding new data and release any
     * related resources as soon as practicable since adding data won't have any downstream effects.
     *
     * <p>
     * Once this is {@code false}, it will always remain {@code false}. For more prompt notifications, callers may
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
