package io.deephaven.engine.readtracker.impl;

/**
 * Helper to accumulate the number of data reads and meta-data operations performed by the I/O subsystem.
 */
public class QueryPerformanceReadTracker {
    /**
     * An object that contains the cumulative data read and metadata read counters for the current thread.
     */
    static final ThreadLocal<ReadTracker> READ_TRACKER = ThreadLocal.withInitial(ReadTracker::new);

    /**
     * Record a data read operation. Accumulates into the current thread's cumulative counters.
     *
     * <p>
     * If the bytes read is less than zero, the read is not recorded.
     * </p>
     *
     * @param nanos time spent on the read in nanoseconds
     * @param bytesRead number of bytes read
     */
    public static void recordRead(final long nanos, final long bytesRead) {
        if (bytesRead < 0) {
            return;
        }
        READ_TRACKER.get().recordRead(nanos, bytesRead);
    }

    /**
     * Record a metadata operation (e.g. listing files, checking existence, determining file sizes). Accumulates into
     * the current thread's cumulative counters.
     *
     * @param nanos time spent on the metadata operation in nanoseconds
     */
    public static void recordMetadataOperation(final long nanos) {
        READ_TRACKER.get().recordMetadataOperation(nanos);
    }

    /**
     * Gets the tracker for the current thread. The tracker is not thread-safe and is mutable. You must capture any
     * values from the tracker rather than holding a reference to the returned object.
     *
     * @return the tracker for the current thread
     */
    public static ReadTracker getReadTrackerForCurrentThread() {
        return READ_TRACKER.get();
    }

    /**
     * Thread local read tracker; no synchronization is required because multiple threads are never permitted to access
     * it simultaneously.
     */
    public static class ReadTracker {
        long readCount;
        long readNanos;
        long readBytes;
        long metadataOperationCount;
        long metadataOperationNanos;

        private void recordRead(final long nanos, final long bytesRead) {
            readCount++;
            readNanos += nanos;
            readBytes += bytesRead;
        }

        private void recordMetadataOperation(final long nanos) {
            metadataOperationCount++;
            metadataOperationNanos += nanos;
        }

        /**
         * Get the cumulative data read nanos for this tracker.
         *
         * @return total data read nanos accumulated for this tracker
         */
        public long getDataReadNanos() {
            return readNanos;
        }

        /**
         * Get the cumulative data read count for this tracker.
         *
         * @return total data read count accumulated on this tracker
         */
        public long getDataReadCount() {
            return readCount;
        }

        /**
         * Get the cumulative data read bytes for this tracker.
         *
         * @return total data read bytes accumulated on this tracker
         */
        public long getDataReadBytes() {
            return readBytes;
        }

        /**
         * Get the cumulative metadata read nanos for this tracker.
         *
         * @return total metadata read nanos accumulated on this tracker
         */
        public long getMetadataOperationNanos() {
            return metadataOperationNanos;
        }

        /**
         * Get the cumulative metadata read count for this tracker.
         *
         * @return total metadata read count accumulated on this tracker
         */
        public long getMetadataOperationCount() {
            return metadataOperationCount;
        }
    }
}
