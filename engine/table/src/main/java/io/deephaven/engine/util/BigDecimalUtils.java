package io.deephaven.engine.util;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

import java.math.BigDecimal;

public class BigDecimalUtils {
    public static class PrecisionAndScale {
        public final int precision;
        public final int scale;

        public PrecisionAndScale(final int precision, final int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }

    public static PrecisionAndScale computePrecisionAndScale(
            final Table t, final String colName) {
        final ColumnSource<BigDecimal> src = t.getColumnSource(colName, BigDecimal.class);
        return computePrecisionAndScale(t.getRowSet(), src);
    }

    public static PrecisionAndScale computePrecisionAndScale(
            final TrackingRowSet rowSet,
            final ColumnSource<BigDecimal> source
    ) {
        final int sz = 4096;
        // we first compute max(precision - scale) and max(scale), which corresponds to
        // max(digits left of the decimal point), max(digits right of the decimal point).
        // Then we convert to (precision, scale) before returning.
        int maxPrecisionMinusScale = 0;
        int maxScale = 0;
        try (final ChunkSource.GetContext context = source.makeGetContext(sz);
             final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            final RowSequence rowSeq = it.getNextRowSequenceWithLength(sz);
            final ObjectChunk<BigDecimal, ? extends Values> chunk = source.getChunk(context, rowSeq).asObjectChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                final BigDecimal x = chunk.get(i);
                final int precision = x.precision();
                final int scale = x.scale();
                final int precisionMinusScale = precision - scale;
                if (precisionMinusScale > maxPrecisionMinusScale) {
                    maxPrecisionMinusScale = precisionMinusScale;
                }
                if (scale > maxScale) {
                    maxScale = scale;
                }
            }
        }
        return new PrecisionAndScale(maxPrecisionMinusScale + maxScale, maxScale);
    }
}
