package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;

/**
 * Regioned column source implementation for columns of {@link DateTime}s.
 */
final class RegionedColumnSourceDateTime
        extends
        RegionedColumnSourceReferencing<DateTime, Values, Long, ColumnRegionLong<Values>>
        implements ColumnSourceGetDefaults.ForObject<DateTime> {

    public RegionedColumnSourceDateTime() {
        super(ColumnRegionLong.createNull(PARAMETERS.regionMask), DateTime.class,
                RegionedColumnSourceLong.NativeType.AsValues::new);
    }

    @Override
    public void convertRegion(WritableChunk<? super Values> destination,
            Chunk<? extends Values> source, RowSequence rowSequence) {
        WritableObjectChunk<DateTime, ? super Values> objectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> longChunk = source.asLongChunk();

        final int size = objectChunk.size();
        final int length = longChunk.size();

        for (int i = 0; i < length; ++i) {
            objectChunk.set(size + i, DateTimeUtils.nanosToTime(longChunk.get(i)));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public DateTime get(long elementIndex) {
        return elementIndex == RowSequence.NULL_ROW_KEY ? null
                : DateTimeUtils.nanosToTime(lookupRegion(elementIndex).getReferencedRegion().getLong(elementIndex));
    }
}
