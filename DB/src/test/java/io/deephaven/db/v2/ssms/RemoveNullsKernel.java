package io.deephaven.db.v2.ssms;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableCharChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;

public interface RemoveNullsKernel {
    /**
     * Remove Deephaven nulls from a sorted chunk.
     *
     * @param chunk the chunk to remove nulls from
     */
    void removeNulls(WritableChunk<? extends Attributes.Any> chunk);

    class CharRemoveNulls implements RemoveNullsKernel {
        @Override
        public void removeNulls(WritableChunk<? extends Attributes.Any> chunk) {
            removeNulls(chunk.asWritableCharChunk());
        }

        public static void removeNulls(WritableCharChunk<? extends Attributes.Any> chunk) {
            int maxCopy = 0;
            if (chunk.get(chunk.size() - 1) == Character.MAX_VALUE) {
                int lastMax = chunk.size() - 1;
                while (lastMax > 0 && chunk.get(lastMax - 1) == Character.MAX_VALUE) {
                    lastMax--;
                }
                maxCopy = chunk.size() - lastMax;
                chunk.setSize(lastMax + 1);
            }
            int sz = chunk.size();
            while (sz > 0 && chunk.get(sz - 1) == QueryConstants.NULL_CHAR) {
                sz--;
            }
            if (maxCopy > 0) {
                chunk.setSize(sz + maxCopy);
                chunk.fillWithValue(sz, maxCopy, Character.MAX_VALUE);
            } else {
                chunk.setSize(sz);
            }
        }
    }
}
