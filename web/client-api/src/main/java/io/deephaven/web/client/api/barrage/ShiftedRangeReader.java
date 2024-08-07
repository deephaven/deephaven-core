//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;

import java.nio.ByteBuffer;
import java.util.PrimitiveIterator;

public class ShiftedRangeReader {

    public static ShiftedRange[] read(ByteBuffer data) {
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

    public static ByteBuffer write(ShiftedRange[] shiftedRanges) {
        RangeSet start = new RangeSet();
        RangeSet end = new RangeSet();
        RangeSet postShiftStart = new RangeSet();

        for (int i = 0; i < shiftedRanges.length; i++) {
            ShiftedRange range = shiftedRanges[i];
            long first = range.getRange().getFirst();
            long last = range.getRange().getLast();
            long delta = range.getDelta() + first;
            start.addRange(new Range(first, first));
            end.addRange(new Range(last, last));
            postShiftStart.addRange(new Range(delta, delta));
        }

        ByteBuffer startBuf = CompressedRangeSetReader.writeRange(start);
        ByteBuffer endBuf = CompressedRangeSetReader.writeRange(end);
        ByteBuffer shiftBuf = CompressedRangeSetReader.writeRange(postShiftStart);
        ByteBuffer all = ByteBuffer.allocateDirect(startBuf.remaining() + endBuf.remaining() + shiftBuf.remaining());
        all.put(startBuf);
        all.put(endBuf);
        all.put(shiftBuf);
        return all;
    }
}
