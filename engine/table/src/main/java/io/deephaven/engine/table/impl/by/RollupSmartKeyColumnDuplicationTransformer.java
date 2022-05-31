package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.impl.RollupInfo;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.SmartKeySource;

import java.util.Arrays;
import java.util.Map;

class RollupSmartKeyColumnDuplicationTransformer implements AggregationContextTransformer {
    private final String[] names;

    RollupSmartKeyColumnDuplicationTransformer(String[] names) {
        this.names = names;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        final ColumnSource[] keySources = Arrays.stream(names).map(resultColumns::get).toArray(ColumnSource[]::new);
        resultColumns.put(RollupInfo.ROLLUP_COLUMN, new SmartKeySource(keySources));
    }
}
