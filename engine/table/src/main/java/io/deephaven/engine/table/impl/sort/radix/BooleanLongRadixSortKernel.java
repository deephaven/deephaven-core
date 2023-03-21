/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sort.radix;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.chunk.*;

public class BooleanLongRadixSortKernel {

    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
    LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> createContext(
            int size,
            SortingOrder order,
            boolean preserveValues) {
        if (order == SortingOrder.Ascending) {
            return new BooleanLongSortKernel<>(size, preserveValues);
        } else {
            return new BooleanLongSortDescendingKernel<>(size, preserveValues);
        }
    }

    private static class BooleanLongSortKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
            implements LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> {

        final WritableLongChunk<PERMUTE_VALUES_ATTR> nullKeys;
        final WritableLongChunk<PERMUTE_VALUES_ATTR> falseKeys;
        private final boolean preserveValues;
        int backPosition = 0;

        private BooleanLongSortKernel(int size, boolean preserveValues) {
            nullKeys = WritableLongChunk.makeWritableChunk(size);
            falseKeys = WritableLongChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort) {
            nullKeys.setSize(0);
            falseKeys.setSize(0);
            doSortAscending(valuesToPermute, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort,
                IntChunk<? extends ChunkPositions> offsetsIn,
                IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                nullKeys.setSize(0);
                falseKeys.setSize(0);
                doSortAscending(valuesToPermute, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii), lengthsIn.get(ii));
            }
        }

        void doSortAscending(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableObjectChunk<Boolean, SORT_VALUES_ATTR> valuesToSort,
                int offset,
                int length) {
            int backCursor = offset + length - 1;
            backPosition = offset + length - 1;

            while (backCursor >= offset) {
                final Boolean val = valuesToSort.get(backCursor);
                if (val == null) {
                    nullKeys.add(valuesToPermute.get(backCursor));
                } else if (!val) {
                    falseKeys.add(valuesToPermute.get(backCursor));
                } else {
                    final long cursorValue = valuesToPermute.get(backCursor);
                    valuesToPermute.set(backPosition--, cursorValue);
                }
                --backCursor;
            }

            final int nullCount = nullKeys.size();
            for (int ii = 0; ii < nullCount; ++ii) {
                valuesToPermute.set(offset + ii, nullKeys.get(nullCount - ii - 1));
            }
            final int falseCount = falseKeys.size();
            for (int ii = 0; ii < falseKeys.size(); ++ii) {
                valuesToPermute.set(offset + ii + nullCount, falseKeys.get(falseCount - ii - 1));
            }
            if (preserveValues) {
                for (int ii = 0; ii < nullCount; ++ii) {
                    valuesToSort.set(offset + ii, null);
                }
                for (int ii = 0; ii < falseCount; ++ii) {
                    valuesToSort.set(offset + nullCount + ii, Boolean.FALSE);
                }
                for (int ii = nullCount + falseCount; ii < length; ++ii) {
                    valuesToSort.set(offset + ii, Boolean.TRUE);
                }
            }
        }

        @Override
        public void close() {
        }
    }

    private static class BooleanLongSortDescendingKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
            implements LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> {

        final WritableLongChunk<Any> falseKeys;
        private final boolean preserveValues;
        final WritableLongChunk<Any> trueKeys;
        int backPosition = 0;

        private BooleanLongSortDescendingKernel(int size, boolean preserveValues) {
            trueKeys = WritableLongChunk.makeWritableChunk(size);
            falseKeys = WritableLongChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort) {
            trueKeys.setSize(0);
            falseKeys.setSize(0);
            doSortDescending(valuesToPermute, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort,
                IntChunk<? extends ChunkPositions> offsetsIn,
                IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                trueKeys.setSize(0);
                falseKeys.setSize(0);
                doSortDescending(valuesToPermute, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii), lengthsIn.get(ii));
            }
        }

        void doSortDescending(
                WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableObjectChunk<Boolean, SORT_VALUES_ATTR> valuesToSort,
                int offset,
                int length) {
            int backCursor = offset + length - 1;
            backPosition = offset + length - 1;

            while (backCursor >= offset) {
                final Boolean val = valuesToSort.get(backCursor);
                if (val == null) {
                    final long cursorValue = valuesToPermute.get(backCursor);
                    valuesToPermute.set(backPosition--, cursorValue);
                } else if (val) {
                    trueKeys.add(valuesToPermute.get(backCursor));
                } else {
                    falseKeys.add(valuesToPermute.get(backCursor));
                }
                --backCursor;

            }

            final int trueCount = trueKeys.size();
            for (int ii = 0; ii < trueCount; ++ii) {
                valuesToPermute.set(offset + ii, trueKeys.get(trueCount - ii - 1));
            }
            final int falseCount = falseKeys.size();
            for (int ii = 0; ii < falseKeys.size(); ++ii) {
                valuesToPermute.set(offset + ii + trueCount, falseKeys.get(falseCount - ii - 1));
            }

            if (preserveValues) {
                for (int ii = 0; ii < trueCount; ++ii) {
                    valuesToSort.set(offset + ii, Boolean.TRUE);
                }
                for (int ii = 0; ii < falseCount; ++ii) {
                    valuesToSort.set(offset + trueCount + ii, Boolean.FALSE);
                }
                for (int ii = trueCount + falseCount; ii < length; ++ii) {
                    valuesToSort.set(offset + ii, null);
                }
            }
        }

        @Override
        public void close() {
        }
    }
}
