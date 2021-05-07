package io.deephaven.db.v2.by;

import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;

import java.util.Map;

class RollupKeyColumnDuplicationTransformer implements AggregationContextTransformer {
    private final String name;

    RollupKeyColumnDuplicationTransformer(String name) {
        this.name = name;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        resultColumns.put(RollupInfo.ROLLUP_COLUMN, resultColumns.get(name));
    }
}
