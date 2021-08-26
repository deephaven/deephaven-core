package io.deephaven.db.v2.utils;

public class RedirectionIndexUtilities {
    private static final long SEARCH_ITERATOR_THRESHOLD = 512;

    static void applyRedirectionShift(final RedirectionIndex redirectionIndex, final ReadOnlyIndex filterIndex,
            final IndexShiftData shiftData) {

        final IndexShiftData.SingleElementShiftCallback applyOneShift = (key, delta) -> {
            final long oldKey = redirectionIndex.remove(key);
            if (oldKey != Index.NULL_KEY) {
                redirectionIndex.putVoid(key + delta, oldKey);
            }
        };

        if (shiftData.getEffectiveSize() >= SEARCH_ITERATOR_THRESHOLD) {
            shiftData.forAllInIndex(filterIndex, applyOneShift);
        } else {
            shiftData.apply((start, end, delta) -> {
                final long len = end - start + 1;
                final boolean negativeShift = delta < 0;
                if (negativeShift) {
                    for (long offset = 0; offset < len; ++offset) {
                        applyOneShift.shift(start + offset, delta);
                    }
                } else {
                    for (long offset = len - 1; offset >= 0; --offset) {
                        applyOneShift.shift(start + offset, delta);
                    }
                }
            });
        }
    }
}
