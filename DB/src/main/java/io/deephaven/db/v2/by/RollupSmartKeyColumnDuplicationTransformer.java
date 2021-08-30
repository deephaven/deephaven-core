package io.deephaven.db.v2.by;

import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.tuples.SmartKeySource;

import java.util.Arrays;
import java.util.Map;

class RollupSmartKeyColumnDuplicationTransformer implements AggregationContextTransformer {
    private final String[] names;

    RollupSmartKeyColumnDuplicationTransformer(String[] names) {
        this.names = names;
    }

    @Override
    public void resultColumnFixup(Map<String, ColumnSource<?>> resultColumns) {
        final ColumnSource[] keySources =
            Arrays.stream(names).map(resultColumns::get).toArray(ColumnSource[]::new);
        resultColumns.put(RollupInfo.ROLLUP_COLUMN, new SmartKeySource(keySources));
    }
}
