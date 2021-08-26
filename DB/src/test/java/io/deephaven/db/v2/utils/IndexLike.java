package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.RspBitmap;

public interface IndexLike {
    interface Factory {
        String name();

        IndexLike make();
    }
    interface ActualIndex extends IndexLike {
        Index getIndex();
    }

    String name();

    void addKey(final long key);

    void addRange(final long start, final long end);

    void doneAdding();

    long lastKey();

    int initialCapacity = 16;

    IndexLike.Factory pqf = new IndexLike.Factory() {
        @Override
        public IndexLike make() {
            return new IndexLike.ActualIndex() {
                RangePriorityQueueBuilder b = new RangePriorityQueueBuilder(initialCapacity);
                Index idx;

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
                    idx = new TreeIndex(b.getTreeIndexImpl());
                    b = null;
                }

                @Override
                public long lastKey() {
                    return idx.lastKey();
                }

                @Override
                public String name() {
                    return RangePriorityQueueBuilder.class.getSimpleName();
                }

                @Override
                public Index getIndex() {
                    return idx;
                }
            };
        }

        @Override
        public String name() {
            return RangePriorityQueueBuilder.class.getSimpleName();
        }
    };

    IndexLike.Factory mixedf = new IndexLike.Factory() {
        @Override
        public IndexLike make() {
            return new ActualIndex() {
                Index.RandomBuilder b = new Index.AdaptiveIndexBuilder();
                Index idx;

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
                    idx = b.getIndex();
                    b = null;
                }

                @Override
                public long lastKey() {
                    return idx.lastKey();
                }

                @Override
                public String name() {
                    return MixedBuilder.class.getSimpleName();
                }

                @Override
                public Index getIndex() {
                    return idx;
                }
            };
        }

        @Override
        public String name() {
            return MixedBuilder.class.getSimpleName();
        }
    };

    IndexLike.Factory rspf = new IndexLike.Factory() {
        @Override
        public IndexLike make() {
            return new IndexLike() {
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
