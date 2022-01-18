package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;

import java.util.function.BiFunction;
import java.util.function.IntFunction;

/**
 * The various QueueWriters ({@link ByteWriter}, {@link IntWriter}, etc.) work in tandem with their corresponding
 * {@link QueueReader}s ({@link QueueReader.ByteReader}, {@link QueueReader.IntReader}, etc), forming a FIFO queue. The
 * QueueWriter writes data, and the {@link QueueReader} reads that data. If the {@link QueueReader} "catches up", it
 * will block until the QueueWriter provides more data, or indicates that it is done (via the {@link #finish()} method.
 * This synchronization is done at "block" granularity, so the {@link QueueReader} can only proceed when the QueueWriter
 * has written at least a "block" of data or is done. We allow multiple independent {@link QueueReader}s to consume the
 * same underlying data. In our implementation this is used so our type inferencer can take a second "pass" over the
 * same input data.
 *
 * In our implementation the {@link DenseStorageWriter} and {@link DenseStorageReader} are built out of various
 * QueueWriters and {@link QueueReader}s. This explains why the semantics of {@link DenseStorageWriter} and
 * {@link DenseStorageReader} are similar to those of the underlying QueueWriters and {@link QueueReader}s.
 */
public class QueueWriter<TARRAY, TREADER> {
    /**
     * Sync object which synchronizes access to the "next" fields of every node in our linked list. Shared with the
     * QueueReader.
     */
    private final Object sync;
    /**
     * Tail of the linked list. We append here when we flush.
     */
    private QueueNode<TARRAY> tail;
    /**
     * Size of the chunks we allocate that we pack data into.
     */
    protected final int blockSize;
    /**
     * Lambda for allocating arrays for our chunks.
     */
    private final IntFunction<TARRAY> arrayFactory;
    /**
     * Lambda to make a QueueReader of the right subtype.
     */
    private final BiFunction<Object, QueueNode<TARRAY>, TREADER> readerFactory;
    /**
     * A flag that says whether it's still early enough to allow QueueReader creation. After the writer starts writing,
     * they shouldn't be allowed to create any readers. (This is just because we want to keep the semantics simple).
     */
    private boolean allowReaderCreation;
    /**
     * Current block we writing to. When we flush, we will write it to a new linked list node.
     */
    private TARRAY genericBlock;
    /**
     * Start of the current block. This is typically 0, but not always. If the caller does an early flush (before the
     * block is filled), you can have multiple linked list nodes sharing different segments of the same underlying block
     * storage.
     */
    protected int begin;
    /**
     * Current offset in the current block. Updated as we write data. When the value reaches "end", then data in this
     * block is exhausted.
     */
    protected int current;
    /**
     * End of the current block. The same as genericBlock.length.
     */
    protected int end;

    /**
     * Constructor.
     */
    protected QueueWriter(final int blockSize,
            final IntFunction<TARRAY> arrayFactory,
            final BiFunction<Object, QueueNode<TARRAY>, TREADER> readerFactory) {
        this.sync = new Object();
        // Creating the linked list with a sentinel object makes linked list manipulation code simpler.
        this.tail = new QueueNode<>(null, 0, 0, false);
        this.blockSize = blockSize;
        this.arrayFactory = arrayFactory;
        this.readerFactory = readerFactory;
        this.allowReaderCreation = true;
        this.genericBlock = null;
        this.begin = 0;
        this.current = 0;
        this.end = 0;
    }

    /**
     * Caller is finished writing.
     */
    public void finish() {
        flush(true);
        genericBlock = null; // hygeine
        begin = 0;
        current = 0;
        end = 0;
    }

    /**
     * Make a {@link QueueReader} corresponding to this QueueWriter. You can make as many {@link QueueReader}s as you
     * want, but you should make them before you start writing data.
     */
    public TREADER newReader() {
        if (!allowReaderCreation) {
            throw new RuntimeException("Logic error: must allocate readers before writing any data");
        }
        return readerFactory.apply(sync, tail);
    }

    /**
     * This supports an "early flush" for callers like {@link DenseStorageWriter} who want to flush all their queues
     * from time to time.
     */
    public void flush() {
        flush(false);
    }

    /**
     * Flush can be called at any time... when the block is empty (and hence nothing to flush), when there's some data,
     * or when the data is full.
     * 
     * @param isLast Whether this is the last node in the linked list.
     */
    private void flush(boolean isLast) {
        // Sometimes our users ask us to flush even if there is nothing to flush.
        // If the block is an "isLast" block, we need to flush it regardless of whether it contains data.
        // Otherwise (if the block is not an "isLast" block), we only flush it if it contains data.
        if (!isLast && (current == begin)) {
            // No need to flush.
            return;
        }

        // No more creating readers after the first flush.
        allowReaderCreation = false;

        final QueueNode<TARRAY> newBlob = new QueueNode<>(genericBlock, begin, current, isLast);
        // If this is an early flush (before the block was filled), the next node may share
        // the same underlying storage array (but disjoint segments of that array) as the current node.
        // To accomplish this, we just advance "begin" to "current" here. At this point in the logic
        // we don't care if that leaves the block with zero capacity (begin == end) or not. The decision
        // to actually start a new block is done by the addXXX code in our subclasses which eventually
        // calls flushAndAllocate.
        begin = current;
        synchronized (sync) {
            tail.next = newBlob;
            tail = newBlob;
            sync.notifyAll();
        }
    }

    /**
     * This method exists as a helper method for a subclass' addXXX method. A typical implementation is in CharWriter:
     *
     * <pre>
     * final int sliceSize = cs.size();
     * final boolean flushHappened = current + sliceSize > end;
     * if (flushHappened) {
     *   typedBlock = flushAndAllocate(sliceSize);
     * }
     * ...
     * </pre>
     *
     * The "flushHappened" variable (which at the point of its definition would be more precisely interpreted as "flush
     * is about to happen") calculates whether the data that currently needs to be written can fit in the current block
     * or not. If it can fit, the code continues on to write its data. If it can't fit, the subclass calls this
     * flushAndAllocate method to flush the current block to the linked list and allocate a new one. The new block so
     * allocated is guaranteed to have at be of size at least 'sizeNeeded'.
     */
    protected final TARRAY flushAndAllocate(int sizeNeeded) {
        flush(false);
        final int capacity = Math.max(blockSize, sizeNeeded);
        genericBlock = arrayFactory.apply(capacity);
        begin = 0;
        current = 0;
        end = capacity;
        return genericBlock;
    }

    /**
     * A QueueWriter specialized for bytes.
     */
    public static final class ByteWriter extends QueueWriter<byte[], QueueReader.ByteReader> {
        private byte[] typedBlock = null;

        public ByteWriter(final int blockSize) {
            super(blockSize, byte[]::new, QueueReader.ByteReader::new);
        }

        /**
         * Add bytes from a ByteSlice to the queue.
         * 
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addBytes(ByteSlice bs) {
            final int sliceSize = bs.size();
            if (sliceSize == 0) {
                return false;
            }
            final boolean flushHappened = current + sliceSize > end;
            if (flushHappened) {
                typedBlock = flushAndAllocate(sliceSize);
            }
            bs.copyTo(typedBlock, current);
            current += sliceSize;
            return flushHappened;
        }
    }

    /**
     * A QueueWriter specialized for ints.
     */
    public static final class IntWriter extends QueueWriter<int[], QueueReader.IntReader> {
        private int[] typedBlock = null;

        public IntWriter(final int blockSize) {
            super(blockSize, int[]::new, QueueReader.IntReader::new);
        }

        /**
         * Add an int to the queue.
         * 
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addInt(int value) {
            final boolean flushHappened = current == end;
            if (flushHappened) {
                typedBlock = flushAndAllocate(1);
            }
            typedBlock[current++] = value;
            return flushHappened;
        }
    }

    /**
     * A QueueWriter specialized for byte arrays.
     */
    public static final class ByteArrayWriter extends QueueWriter<byte[][], QueueReader.ByteArrayReader> {
        private byte[][] block = null;

        public ByteArrayWriter(int blobSize) {
            super(blobSize, byte[][]::new, QueueReader.ByteArrayReader::new);
        }

        /**
         * Add a byte array to the queue.
         * 
         * @return true if the add caused a flush to happen prior to the write, false if no flush happened.
         */
        public boolean addByteArray(byte[] value) {
            final boolean flushHappened = current == end;
            if (flushHappened) {
                block = flushAndAllocate(1);
            }
            block[current++] = value;
            return flushHappened;
        }
    }
}
