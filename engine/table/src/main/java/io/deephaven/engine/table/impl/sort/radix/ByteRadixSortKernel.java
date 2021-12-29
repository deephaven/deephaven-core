package io.deephaven.engine.table.impl.sort.radix;

import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

class ByteRadixSortKernel {
    public static LongSortKernel createContext(int size, SortingOrder order, boolean last) {
        return new ByteRadixSortContext(size, last, order);
    }

    private static class ByteRadixSortContext<ATTR extends Any, KEY_INDICES extends RowKeys> implements LongSortKernel<ATTR, KEY_INDICES> {
        final WritableLongChunk<KEY_INDICES> [] keys;
        private final int size;
        private final boolean last;
        private final SortingOrder order;

        private ByteRadixSortContext(int size, boolean last, SortingOrder order) {
            this.size = size;
            this.last = last;
            this.order = order;
            //noinspection unchecked
            keys = new WritableLongChunk[(int)Byte.MAX_VALUE - (int)Byte.MIN_VALUE];
            for (int ii = 0; ii < keys.length; ++ii) {
                keys[ii] = WritableLongChunk.makeWritableChunk(size);
            }
        }

        private void doBucket(WritableLongChunk<KEY_INDICES> indexKeys, ByteChunk<ATTR> valuesToSort, int start, int length) {
            for (int ii = start; ii < start + length; ++ii) {
                final byte value = valuesToSort.get(ii);
                keys[(int)value - Byte.MIN_VALUE].add(indexKeys.get(ii));
            }
        }

        private void zeroSize() {
            for (int ii = 0; ii < keys.length; ++ii) {
                keys[ii].setSize(0);
            }
        }


        @Override
        public void sort(WritableLongChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort) {
            zeroSize();
            doBucket(indexKeys, valuesToSort.asByteChunk(), 0, valuesToSort.size());
            if (order == SortingOrder.Ascending) {
                ascendingBuckets(indexKeys, valuesToSort.asWritableByteChunk(), 0, valuesToSort.size());
            } else {
                descendingBuckets(indexKeys, valuesToSort.asWritableByteChunk(), 0, valuesToSort.size());
            }
        }

        @Override
        public void sort(WritableLongChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort, IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                zeroSize();
                final int start = offsetsIn.get(ii);
                final int length = lengthsIn.get(ii);
                doBucket(indexKeys, valuesToSort.asByteChunk(), start, length);
                if (order == SortingOrder.Ascending) {
                    ascendingBuckets(indexKeys, valuesToSort.asWritableByteChunk(), start, length);
                } else {
                    descendingBuckets(indexKeys, valuesToSort.asWritableByteChunk(), start, length);
                }
            }
        }

        void ascendingBuckets(WritableLongChunk<KEY_INDICES> indexKeys, WritableByteChunk<ATTR> valuesToSort, int start, int length) {
            int indexPos = start;
            for (int ii = 0; ii < keys.length; ++ii) {
                if (keys[ii].size() > 0) {
                    indexKeys.copyFromChunk(keys[ii], 0, indexPos, keys[ii].size());
                    indexPos += keys[ii].size();
                }
            }
            if (last) {
                int valuePos = start;
                for (int ii = 0; ii < keys.length; ++ii) {
                    final int keySize = keys[ii].size();
                    if (keySize > 0) {
                        for (int jj = valuePos; jj < valuePos + keySize; ++jj) {
                            valuesToSort.set(jj, (byte)(ii - Byte.MIN_VALUE));
                        }
                        valuePos += keySize;
                    }
                }
            }
        }

        void descendingBuckets(WritableLongChunk<KEY_INDICES> indexKeys, WritableByteChunk<ATTR> valuesToSort, int start, int length) {
            int indexPos = start;
            for (int ii = keys.length - 1; ii >= 0; --ii) {
                if (keys[ii].size() > 0) {
                    indexKeys.copyFromChunk(keys[ii], 0, indexPos, keys[ii].size());
                    indexPos += keys[ii].size();
                }
            }
            if (last) {
                int valuePos = start;
                for (int ii = keys.length - 1; ii >= 0; --ii) {
                    final int keySize = keys[ii].size();
                    if (keySize > 0) {
                        for (int jj = valuePos; jj < valuePos + keySize; ++jj) {
                            valuesToSort.set(jj, (byte)(ii - Byte.MIN_VALUE));
                        }
                        valuePos += keySize;
                    }
                }
            }
        }

        @Override
        public void close() {
        }
    }
}