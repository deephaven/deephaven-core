package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.ShiftAwareListener;

/**
 * Calculate the size of the chunks needed to process an update.
 *
 * The assumption is that the operation processes removed, modified, and added values sequentially
 * (not concurrently); so the largest add/modified/removed index is all that is needed at one time.
 * The effective shifts size is also included in the required update size.
 */
public class UpdateSizeCalculator {
    /**
     * Return the size of chunk needed to process this update (removed, modified, then added
     * sequentially not concurrently).
     *
     * @param upstream the update to process
     * @param chunkSize the maximum chunk size (a maximum for our size)
     *
     * @return an appropriate maximum chunk size for this update
     */
    public static int chunkSize(ShiftAwareListener.Update upstream, int chunkSize) {
        final long updateSize = Math.max(Math.max(upstream.added.size(), upstream.removed.size()),
            upstream.modified.size());
        return chunkSize(updateSize, upstream.shifted, chunkSize);
    }

    /**
     * Return the size of chunk needed to process the shifts in this update update.
     *
     * @param updateSize the existing update size (a minimum for our size)
     * @param shifted the shift to get the effective size for (clamped to chunkSize)
     * @param chunkSize the maximum chunk size (a maximum for our size)
     *
     * @return an appropriate maximum chunk size for this update
     */
    public static int chunkSize(long updateSize, IndexShiftData shifted, int chunkSize) {
        if (updateSize >= chunkSize) {
            return chunkSize;
        }
        return (int) Math.min(chunkSize,
            Math.max(updateSize, shifted.getEffectiveSizeClamped(chunkSize)));
    }
}
