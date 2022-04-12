/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BooleanLongRadixSortKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.radix;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.chunk.*;

public class BooleanIntRadixSortKernel {
    public static <ATTR extends Any, KEY_INDICES extends Indices> IntSortKernel<ATTR, KEY_INDICES> createContext(int size, SortingOrder order, boolean preserveValues) {
        if (order == SortingOrder.Ascending) {
            return new BooleanIntSortKernel<>(size, preserveValues);
        } else {
            return new BooleanIntSortDescendingKernel<>(size, preserveValues);
        }
    }

    private static class BooleanIntSortKernel<ATTR extends Any, KEY_INDICES extends Indices> implements IntSortKernel<ATTR, KEY_INDICES> {
        final WritableIntChunk<KEY_INDICES> nullKeys;
        final WritableIntChunk<KEY_INDICES> falseKeys;
        private final boolean preserveValues;
        int backPosition = 0;

        private BooleanIntSortKernel(int size, boolean preserveValues) {
            nullKeys = WritableIntChunk.makeWritableChunk(size);
            falseKeys = WritableIntChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort) {
            nullKeys.setSize(0);
            falseKeys.setSize(0);
            doSortAscending(indexKeys, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort, IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                nullKeys.setSize(0);
                falseKeys.setSize(0);
                doSortAscending(indexKeys, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii), lengthsIn.get(ii));
            }
        }

        void doSortAscending(WritableIntChunk<KEY_INDICES> indexKeys, WritableObjectChunk<Boolean, ATTR> valuesToSort, int offset, int length) {
            int backCursor = offset + length - 1;
            backPosition = offset + length - 1;

            while (backCursor >= offset) {
                final Boolean val = valuesToSort.get(backCursor);
                if (val == null) {
                    nullKeys.add(indexKeys.get(backCursor));
                } else if (!val) {
                    falseKeys.add(indexKeys.get(backCursor));
                } else {
                    final int cursorValue = indexKeys.get(backCursor);
                    indexKeys.set(backPosition--, cursorValue);
                }
                --backCursor;
            }

            final int nullCount = nullKeys.size();
            for (int ii = 0; ii < nullCount; ++ii) {
                indexKeys.set(offset + ii, nullKeys.get(nullCount - ii - 1));
            }
            final int falseCount = falseKeys.size();
            for (int ii = 0; ii < falseKeys.size(); ++ii) {
                indexKeys.set(offset + ii + nullCount, falseKeys.get(falseCount - ii - 1));
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

    private static class BooleanIntSortDescendingKernel<ATTR extends Any, KEY_INDICES extends Indices> implements IntSortKernel<ATTR, KEY_INDICES> {
        final WritableIntChunk<Indices> falseKeys;
        private final boolean preserveValues;
        final WritableIntChunk<Indices> trueKeys;
        int backPosition = 0;

        private BooleanIntSortDescendingKernel(int size, boolean preserveValues) {
            trueKeys = WritableIntChunk.makeWritableChunk(size);
            falseKeys = WritableIntChunk.makeWritableChunk(size);
            this.preserveValues = preserveValues;
        }

        @Override
        public void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort) {
            trueKeys.setSize(0);
            falseKeys.setSize(0);
            doSortDescending(indexKeys, valuesToSort.asWritableObjectChunk(), 0, valuesToSort.size());
        }

        @Override
        public void sort(WritableIntChunk<KEY_INDICES> indexKeys, WritableChunk<ATTR> valuesToSort, IntChunk<? extends ChunkPositions> offsetsIn, IntChunk<? extends ChunkLengths> lengthsIn) {
            for (int ii = 0; ii < offsetsIn.size(); ++ii) {
                trueKeys.setSize(0);
                falseKeys.setSize(0);
                doSortDescending(indexKeys, valuesToSort.asWritableObjectChunk(), offsetsIn.get(ii), lengthsIn.get(ii));
            }
        }

        void doSortDescending(WritableIntChunk<KEY_INDICES> indexKeys, WritableObjectChunk<Boolean, ATTR> valuesToSort, int offset, int length) {
            int backCursor = offset + length - 1;
            backPosition = offset + length - 1;

            while (backCursor >= offset) {
                final Boolean val = valuesToSort.get(backCursor);
                if (val == null) {
                    final int cursorValue = indexKeys.get(backCursor);
                    indexKeys.set(backPosition--, cursorValue);
                } else if (val) {
                    trueKeys.add(indexKeys.get(backCursor));
                } else {
                    falseKeys.add(indexKeys.get(backCursor));
                }
                --backCursor;

            }

            final int trueCount = trueKeys.size();
            for (int ii = 0; ii < trueCount; ++ii) {
                indexKeys.set(offset + ii, trueKeys.get(trueCount - ii - 1));
            }
            final int falseCount = falseKeys.size();
            for (int ii = 0; ii < falseKeys.size(); ++ii) {
                indexKeys.set(offset + ii + trueCount, falseKeys.get(falseCount - ii - 1));
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
