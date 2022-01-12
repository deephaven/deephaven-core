package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Companion to the {@link QueueWriter}. See the documentation there for details.
 */
public class QueueReader<TARRAY> {
    /**
     * Sync object which synchronizes access to the "next" fields of every node in our linked list. Shared with the
     * QueueWriter.
     */
    private final Object sync;
    /**
     * Current node.
     */
    private QueueNode<TARRAY> node;
    /**
     * Current block we are reading from, extracted from the current node.
     */
    protected TARRAY genericBlock;
    /**
     * Current offset in the current block. Updated as we read data. When the value reaches "end", then data in this
     * block is exhausted.
     */
    protected int current;
    /**
     * "end" offset of the current block.
     */
    protected int end;

    /**
     * Constructor.
     */
    protected QueueReader(Object sync, QueueNode<TARRAY> node) {
        this.sync = sync;
        this.node = node;
        this.genericBlock = null;
        this.current = 0;
        this.end = 0;
    }

    /**
     * This method exists as a helper method for a subclass' tryGetXXX method. A typical implementation is in
     * CharReader:
     *
     * <pre>
     * <code>
     * if (current + size > end) {
     *     if (!tryRefill(size)) {
     *         return false;
     *     }
     *     typedBlock = genericBlock;
     * }
     * </code>
     * </pre>
     *
     * The "if" in the caller is actually checking for multiple cases in a single comparison. One is a normal "buffer
     * empty, needs to be refilled" case. The other is a bad "something went terribly wrong" case.
     *
     * <ul>
     * <li>Case 1, The "buffer empty" case. Then current == end, and therefore current + size > end (assuming size > 0,
     * which it always is). Therefore, the 'if' inside the tryGetXXX code would evaluate to true, so the tryGetXXX code
     * would call this method. Then this method refills the buffer.</li>
     *
     * <li>Case 2: The buffer is not empty, but A logic error (which can't happen if the code is correct) has caused the
     * requested slice to go past the end of the block. Then current < end but current + size > end. Again, the 'if'
     * inside the tryGetXXX code would evaluate to true, so the tryGetXXX code would call this method. But then the
     * first line of our method detects the past-the-end condition and throws an exception.</li>
     *
     * <li>Case 3: The "buffer can satisfy the request" case. Then current + size &lt;= end, so the 'if' inside the
     * tryGetXXX code would evaluate to false, and the tryGetXXX method doesn't call this method.</li>
     * </ul>
     */
    protected boolean tryRefill(int size) {
        if (current != end) {
            throw new RuntimeException("Logic error: slice straddled block");
        }
        while (current == end) {
            if (node.isLast) {
                // Hygeine.
                node = null;
                genericBlock = null;
                current = 0;
                end = 0;
                return false;
            }
            synchronized (sync) {
                while (node.next == null) {
                    catchyWait(sync);
                }
                node = node.next;
                genericBlock = node.data;
                current = node.begin;
                end = node.end;
            }
        }
        if (end - current < size) {
            throw new RuntimeException(String.format("Logic error: got short block: expected at least %d, got %d",
                    size, end - current));
        }
        return true;
    }

    /**
     * Call Object.wait() but suppress the need to deal with checked InterruptedExceptions.
     */
    private static void catchyWait(Object o) {
        try {
            o.wait();
        } catch (InterruptedException ie) {
            throw new RuntimeException("Logic error: thread interrupted: can't happen");
        }
    }

    /**
     * A QueueReader specialized for bytes.
     */
    public static final class ByteReader extends QueueReader<byte[]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private byte[] typedBlock;

        /**
         * Constructor.
         */
        public ByteReader(final Object sync, final QueueNode<byte[]> head) {
            super(sync, head);
        }

        /**
         * Tries to get the next ByteSlice from the reader.
         * 
         * @param size The exact number of chars to place in the slice.
         * @param bs The result, modified in place.
         * @return true If the next ByteSlice was successfully read; false if the end of input was reached.
         */
        public boolean tryGetBytes(final int size, final ByteSlice bs) {
            if (current + size > end) {
                if (!tryRefill(size)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            bs.reset(typedBlock, current, current + size);
            current += size;
            return true;
        }
    }

    /**
     * A QueueReader specialized for ints.
     */
    public static final class IntReader extends QueueReader<int[]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private int[] typedBlock;

        /**
         * Constructor.
         */
        public IntReader(Object sync, QueueNode<int[]> head) {
            super(sync, head);
        }

        /**
         * Tries to get the next integer from the reader.
         * 
         * @param result If the operation succeeds, contains the next integer. Otherwise, the contents are unspecified.
         * @return true if the next value was successfully read; false if the end of input was reached.
         */
        public boolean tryGetInt(final MutableInt result) {
            if (current == end) {
                if (!tryRefill(1)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            result.setValue(typedBlock[current++]);
            return true;
        }
    }

    /**
     * A QueueReader specialized for byte arrays.
     */
    public static final class ByteArrayReader extends QueueReader<byte[][]> {
        /**
         * Typed version of the current block. Saves us some implicit casting from the generic TARRAY object. This is a
         * performance optimization that may not matter.
         */
        private byte[][] typedBlock;

        public ByteArrayReader(final Object sync, final QueueNode<byte[][]> head) {
            super(sync, head);
        }

        /**
         * Tries to get the next ByteSlice from the reader.
         * 
         * @param bs The result, modified in place.
         * @return true If the next ByteSlice was successfully read; false if the end of input was reached.
         */
        public boolean tryGetBytes(final ByteSlice bs) {
            if (current == end) {
                if (!tryRefill(1)) {
                    return false;
                }
                typedBlock = genericBlock;
            }
            final byte[] data = typedBlock[current++];
            bs.reset(data, 0, data.length);
            return true;
        }
    }
}
