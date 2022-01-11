package io.deephaven.csv.densestorage;

/**
 * Linked list node that holds data for a {@link DenseStorageWriter} or {@link DenseStorageReader}. All fields are
 * immutable except the "next" field. Synchronization for reading/writing the "next" field is managed by the
 * {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public final class QueueNode<TARRAY> {
    public final TARRAY data;
    public final int begin;
    public final int end;
    public final boolean isLast;
    /**
     * Readers and writers of this field have arranged to synchronize with each other.
     */
    public QueueNode<TARRAY> next;

    /**
     * Constructor. Sets this queue node to represent the half-open interval ['begin','end') of the array 'data'.
     */
    public QueueNode(TARRAY data, int begin, int end, boolean isLast) {
        this.data = data;
        this.begin = begin;
        this.end = end;
        this.isLast = isLast;
        this.next = null;
    }
}
