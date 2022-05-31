package io.deephaven.util.datastructures;

public interface LongRangeIterator {

    boolean hasNext();

    long start();

    long end(); // inclusive

    void next();

    default boolean forEachLongRange(final LongRangeAbortableConsumer lrc) {
        while (hasNext()) {
            next();
            final long s = start();
            final long e = end();
            if (!lrc.accept(s, e)) {
                return false;
            }
        }
        return true;
    }

    default void forAllLongRanges(final LongRangeConsumer lrc) {
        forEachLongRange((final long first, final long last) -> {
            lrc.accept(first, last);
            return true;
        });
    }
}
