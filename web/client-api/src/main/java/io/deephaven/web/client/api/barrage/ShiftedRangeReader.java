package io.deephaven.web.client.api.barrage;

import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator;

public class ShiftedRangeReader {

    public ShiftedRange[] read(ByteBuffer data) {
        RangeSet start = new CompressedRangeSetReader().read(data);
        RangeSet end = new CompressedRangeSetReader().read(data);
        RangeSet postShiftStart = new CompressedRangeSetReader().read(data);

        PrimitiveIterator.OfLong startIter = start.indexIterator();
        PrimitiveIterator.OfLong endIter = end.indexIterator();
        PrimitiveIterator.OfLong postShiftStartIter = postShiftStart.indexIterator();

        ShiftedRange[] ranges = new ShiftedRange[0];
        while (startIter.hasNext()) {
            long startPosition = startIter.nextLong();
            ranges[ranges.length] = new ShiftedRange(new Range(startPosition, endIter.nextLong()),
                    postShiftStartIter.nextLong() - startPosition);
        }

        return ranges;
    }
}
