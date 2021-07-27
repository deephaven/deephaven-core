package io.deephaven.stream;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableRegistrar;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter for converting streams of data into columnar Deephaven {@link Table tables}.
 */
public class StreamToTableAdapter implements SafeCloseable, LiveTable, StreamConsumer {

    private final TableDefinition tableDefinition;
    private final StreamPublisher streamPublisher;
    private final LiveTableRegistrar liveTableRegistrar;

    private final QueryTable table;

    public StreamToTableAdapter(@NotNull final TableDefinition tableDefinition,
                                @NotNull final StreamPublisher streamPublisher,
                                @NotNull final LiveTableRegistrar liveTableRegistrar) {
        this.tableDefinition = tableDefinition;
        this.streamPublisher = streamPublisher;
        this.liveTableRegistrar = liveTableRegistrar;
        streamPublisher.register(this);
        liveTableRegistrar.addTable(this);
        // TODO: Construct table using the supplied definition and a series of chunk-wrapping `SwitchColumnSource`s.
        table = null;
    }

    public Table table() {
        return table;
    }

    @Override
    public void close() {
        liveTableRegistrar.removeTable(this);
    }

    @Override
    public void refresh() {
        streamPublisher.flush();
        // Switch columns, update index, deliver notification
    }

    @SafeVarargs
    @Override
    public final void accept(@NotNull final WritableChunk<Attributes.Values>... data) {
        // Accumulate data into buffered column sources
    }
}
