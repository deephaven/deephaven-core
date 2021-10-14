package io.deephaven.engine.v2.by;

import io.deephaven.engine.v2.RollupInfo;
import io.deephaven.engine.v2.sources.ColumnSource;

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
