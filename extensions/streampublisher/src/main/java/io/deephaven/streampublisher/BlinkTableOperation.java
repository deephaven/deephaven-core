package io.deephaven.streampublisher;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;

import java.util.Objects;

public class BlinkTableOperation implements TableType.Visitor<Table> {
    private final Table blinkTable;

    public BlinkTableOperation(Table blinkTable) {
        this.blinkTable = Objects.requireNonNull(blinkTable);
    }

    @Override
    public Table visit(TableType.Blink blink) {
        return blinkTable;
    }

    @Override
    public Table visit(TableType.Append append) {
        return BlinkTableTools.blinkToAppendOnly(blinkTable);
    }

    @Override
    public Table visit(TableType.Ring ring) {
        return RingTableTools.of(blinkTable, ring.capacity());
    }
}
