package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;

/**
 * Wrapper exception class for failures in
 * {@link HierarchicalTable#snapshot(HierarchicalTable.SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * snapshot}.
 */
public class HierarchicalTableSnapshotException extends UncheckedDeephavenException {

    public HierarchicalTableSnapshotException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
