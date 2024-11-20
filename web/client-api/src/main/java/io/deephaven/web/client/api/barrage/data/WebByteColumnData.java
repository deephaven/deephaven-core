//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit WebCharColumnData and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.web.client.api.barrage.data;

import elemental2.core.JsArray;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

public class WebByteColumnData extends WebColumnData {
    private JsArray<Any> tmpStorage;

    @Override
    public void fillChunk(Chunk<?> data, PrimitiveIterator.OfLong destIterator) {
        ByteChunk<?> byteChunk = data.asByteChunk();
        int i = 0;
        while (destIterator.hasNext()) {
            byte value = byteChunk.get(i++);
            arr.setAt((int) destIterator.nextLong(), value == QueryConstants.NULL_BYTE ? null : Js.asAny(value));
        }
    }

    @Override
    public void applyUpdate(
            final List<Chunk<Values>> data,
            final RangeSet added,
            final RangeSet removed) {
        // ensure tmpStorage exists
        if (tmpStorage == null) {
            tmpStorage = new JsArray<>();
        }
        final int newLength = (int) (length - removed.size() + added.size());

        int destOffset = 0;
        int retainSourceOffset = 0;
        int chunkSourceOffset = 0;
        final Iterator<Range> addIter = added.rangeIterator();
        final Iterator<Range> removeIter = removed.rangeIterator();
        final Iterator<Chunk<Values>> dataIter = data.iterator();

        Range nextAdd = addIter.hasNext() ? addIter.next() : null;
        Range nextRemove = removeIter.hasNext() ? removeIter.next() : null;
        ByteChunk<Values> byteChunk = dataIter.hasNext() ? dataIter.next().asByteChunk() : null;
        while (destOffset < newLength) {
            if (nextRemove != null && nextRemove.getFirst() == retainSourceOffset) {
                // skip the range from the source chunk
                retainSourceOffset += (int) nextRemove.size();
                nextRemove = removeIter.hasNext() ? removeIter.next() : null;
            } else if (nextAdd != null && nextAdd.getFirst() == destOffset) {
                // copy the range from the source chunk
                long size = nextAdd.size();
                for (long ii = 0; ii < size; ++ii) {
                    while (byteChunk != null && chunkSourceOffset == byteChunk.size()) {
                        byteChunk = dataIter.hasNext() ? dataIter.next().asByteChunk() : null;
                        chunkSourceOffset = 0;
                    }
                    assert byteChunk != null;
                    byte value = byteChunk.get(chunkSourceOffset++);
                    tmpStorage.setAt(destOffset++, value == QueryConstants.NULL_BYTE ? null : Js.asAny(value));
                }
                nextAdd = addIter.hasNext() ? addIter.next() : null;
            } else {
                // copy the range from the source chunk
                long size = (nextRemove == null ? length : nextRemove.getFirst()) - retainSourceOffset;
                if (nextAdd != null) {
                    size = Math.min(size, nextAdd.getFirst() - destOffset);
                }
                for (long ii = 0; ii < size; ++ii) {
                    tmpStorage.setAt(destOffset++, arr.getAt(retainSourceOffset++));
                }
            }
        }

        // swap arrays to avoid copying and garbage collection
        JsArray<Any> tmp = arr;
        arr = tmpStorage;
        tmpStorage = tmp;
        length = newLength;
    }
}
