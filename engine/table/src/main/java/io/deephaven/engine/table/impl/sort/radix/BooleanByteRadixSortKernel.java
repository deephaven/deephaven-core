//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit BooleanLongRadixSortKernel and run "./gradlew replicateSortKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.radix;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.ByteSortKernel;
import io.deephaven.chunk.*;

public class BooleanByteRadixSortKernel {

    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> ByteSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> createContext(
            int size,
            SortingOrder order,
            boolean preserveValues) {
        if (order == SortingOrder.Ascending) {
            return new BooleanByteSortKernel<>(size, preserveValues);
        } else {
            return new BooleanByteSortDescendingKernel<>(size, preserveValues);
        }
    }

    private static class BooleanByteSortKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
            implements ByteSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> {

        final WritableByteChunk<PERMUTE_VALUES_ATTR> nullKeys;
        final WritableByteChunk<PERMUTE_VALUES_ATTR> falseKeys;
        private final boolean preserveValues;
        int backPosition = 0;

        private BooleanByteSortKernel(int size, boolean preserveValues) {
            nullKeys = WritableByteChunk.makeWritableChunk(size);
            falseKeys = WritableByteChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort) {
            nullKeys.setSize(0);
            falseKeys.setSize(0);
            doSortAscending(valuesToPermute, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort,
                IntChunk<? extends ChunkPositions> offsetsIn,
                IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                nullKeys.setSize(0);
                falseKeys.setSize(0);
                doSortAscending(valuesToPermute, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii),
                        lengthsIn.get(ii));
            }
        }

        void doSortAscending(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
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
                    final byte cursorValue = valuesToPermute.get(backCursor);
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
        public void close() {}
    }

    private static class BooleanByteSortDescendingKernel<SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any>
            implements ByteSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> {

        final WritableByteChunk<Any> falseKeys;
        private final boolean preserveValues;
        final WritableByteChunk<Any> trueKeys;
        int backPosition = 0;

        private BooleanByteSortDescendingKernel(int size, boolean preserveValues) {
            trueKeys = WritableByteChunk.makeWritableChunk(size);
            falseKeys = WritableByteChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort) {
            trueKeys.setSize(0);
            falseKeys.setSize(0);
            doSortDescending(valuesToPermute, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableChunk<SORT_VALUES_ATTR> valuesToSort,
                IntChunk<? extends ChunkPositions> offsetsIn,
                IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                trueKeys.setSize(0);
                falseKeys.setSize(0);
                doSortDescending(valuesToPermute, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii),
                        lengthsIn.get(ii));
            }
        }

        void doSortDescending(
                WritableByteChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
                WritableObjectChunk<Boolean, SORT_VALUES_ATTR> valuesToSort,
                int offset,
                int length) {
            int backCursor = offset + length - 1;
            backPosition = offset + length - 1;

            while (backCursor >= offset) {
                final Boolean val = valuesToSort.get(backCursor);
                if (val == null) {
                    final byte cursorValue = valuesToPermute.get(backCursor);
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
        public void close() {}
    }
}
