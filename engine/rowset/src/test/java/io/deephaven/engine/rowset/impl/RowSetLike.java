/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;

public interface RowSetLike {
    interface Factory {
        String name();

        RowSetLike make();
    }
    interface ActualRowSet extends RowSetLike {
        RowSet getRowSet();
    }

    String name();

    void addKey(final long key);

    void addRange(final long start, final long end);

    void doneAdding();

    long lastKey();

    int initialCapacity = 16;

    RowSetLike.Factory pqf = new RowSetLike.Factory() {
        @Override
        public RowSetLike make() {
            return new ActualRowSet() {
                RangePriorityQueueBuilder b = new RangePriorityQueueBuilder(initialCapacity);
                RowSet idx;

                @Override
                public void addKey(final long key) {
                    b.addKey(key);
                }

                @Override
                public void addRange(final long start, final long end) {
                    b.addRange(start, end);
                }

                @Override
                public void doneAdding() {
                    idx = new WritableRowSetImpl(b.getOrderedLongSet());
                    b = null;
                }

                @Override
                public long lastKey() {
                    return idx.lastRowKey();
                }

                @Override
                public String name() {
                    return RangePriorityQueueBuilder.class.getSimpleName();
                }

                @Override
                public RowSet getRowSet() {
                    return idx;
                }
            };
        }

        @Override
        public String name() {
            return RangePriorityQueueBuilder.class.getSimpleName();
        }
    };

    RowSetLike.Factory mixedf = new RowSetLike.Factory() {
        @Override
        public RowSetLike make() {
            return new ActualRowSet() {
                RowSetBuilderRandom b = new AdaptiveRowSetBuilderRandom();
                RowSet idx;

                @Override
                public void addKey(final long key) {
                    b.addKey(key);
                }

                @Override
                public void addRange(final long start, final long end) {
                    b.addRange(start, end);
                }

                @Override
                public void doneAdding() {
                    idx = b.build();
                    b = null;
                }

                @Override
                public long lastKey() {
                    return idx.lastRowKey();
                }

                @Override
                public String name() {
                    return MixedBuilderRandom.class.getSimpleName();
                }

                @Override
                public RowSet getRowSet() {
                    return idx;
                }
            };
        }

        @Override
        public String name() {
            return MixedBuilderRandom.class.getSimpleName();
        }
    };

    RowSetLike.Factory rspf = new RowSetLike.Factory() {
        @Override
        public RowSetLike make() {
            return new RowSetLike() {
                final RspBitmap rb = new RspBitmap();

                @Override
                public String name() {
                    return RspBitmap.class.getSimpleName();
                }

                @Override
                public void addKey(final long key) {
                    rb.addUnsafeNoWriteCheck(key);
                }

                @Override
                public void addRange(final long start, final long end) {
                    rb.addRangeUnsafeNoWriteCheck(start, end);
                }

                @Override
                public void doneAdding() {
                    rb.finishMutationsAndOptimize();
                }

                @Override
                public long lastKey() {
                    return rb.last();
                }
            };
        }

        @Override
        public String name() {
            return RspBitmap.class.getSimpleName();
        }
    };
}
