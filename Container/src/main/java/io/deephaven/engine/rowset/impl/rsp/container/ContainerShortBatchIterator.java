package io.deephaven.engine.rowset.impl.rsp.container;

public interface ContainerShortBatchIterator {
    /**
     * Writes to buffer the next batch of values, and returns how much of the buffer was used.
     *
     * @param buffer the buffer to write values onto.
     * @param offset first position to start writing in buffer.
     * @param maxCount max number of elements to write to buffer.
     * @return how many values were written.
     */
    int next(short[] buffer, int offset, int maxCount);

    /**
     * Whether the underlying container is exhausted or not
     *
     * @return true if there is data remaining
     */
    boolean hasNext();

    /**
     * Starting from the next iterator position (if any), feed values to the consumer until it returns false. After each
     * value is consumed, the current iterator position is moving forward; eg, a call to forEach that consumes 4
     * elements effectively works as if next was called 4 times.
     *
     * @param sc a ShortConsumer to feed values to.
     * @return false if the processing was stopped by the consumer returning false, true otherwise.
     */
    boolean forEach(ShortConsumer sc);
}
