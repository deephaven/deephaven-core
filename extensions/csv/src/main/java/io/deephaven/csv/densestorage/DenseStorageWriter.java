package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.tokenization.RangeTests;

/**
 * The DenseStorageWriter and {@link DenseStorageReader} work in tandem, forming a FIFO queue. The DenseStorageWriter
 * writes data, and the {@link DenseStorageReader} reads that data. If the {@link DenseStorageReader} "catches up", it
 * will block until the DenseStorageWriter provides more data, or indicates that it is done (via the {@link #finish()}
 * method. This synchronization is done at "block" granularity, so the DenseStorageReader can only proceed when the
 * DenseStorageWriter has written at least a "block" of data or is done. We allow multiple independent
 * {@link DenseStorageReader}s to consume the same underlying data. In our implementation this is used so our type
 * inferencer can take a second "pass" over the same input data.
 *
 * <p>
 * The point of this object is to store a sequence of (character sequences aka "strings", but not java.lang.String),
 * using a small fraction of overhead. The problem with storing every character sequence as a java.lang.String is:
 * <ol>
 * <li>Per-object overhead (probably 8 or 16 bytes depending on pointer width)</li>
 * <li>The memory cost of holding a reference to that String (again 4 or 8 bytes)</li>
 * <li>The string has to know its length (4 bytes)</li>
 * <li>Java characters are 2 bytes even though in practice many strings are ASCII-only and their chars can fit in a
 * byte. (Newer Java implementations can store text as bytes, eliminating this objection)</li>
 * </ol>
 *
 * <p>
 * For small strings (say the word "hello" or the input text "12345.6789") the overhead can be 100% or worse.
 *
 * For our purposes we:
 * <ol>
 * <li>Only need sequential access. i.e. we don't need random access into the sequence of "strings". So we can support a
 * model where we can have a forward-only cursor moving over the sequence of "strings".</li>
 * <li>Don't need to give our caller a data structure that they can hold on to. The caller only gets a "view" (a slice)
 * of the current "string" data. The view is invalidated when they move to the next "string"</li>
 * </ol>
 *
 * Furthermore we:
 * <ol>
 * <li>Offer a FIFO model where the reader (in a separate thread) can chase the writer but there is not an inordinate
 * amount of synchronization overhead (synchronization happens at the block level, not the "string" level).</li>
 * <li>Have the ability to make multiple Readers which pass over the same underlying data. This is our low-drama way of
 * allowing our client to make multiple passes over the data, without complicating the iteration interface, with, e.g.,
 * a reset method.</li>
 * <li>Use a linked-list structure so that when all existing readers have move passed a block of data, that block can be
 * freed by the garbage collector without any explicit action taken by the reader.</li>
 * </ol>
 *
 * If you are familiar with the structure of our inference, you may initially think that this reader-chasing-writer
 * garbage collection trick doesn't buy us much because we have a two-phase parser. However, when the inferencer has
 * gotten to the last parser in its set of allowable parsers (say, the String parser), or the user has specified that
 * there is only one parser for this column, then the code doesn't need to do any inference and can parse the column in
 * one pass. In this case, when the reader stays caught up with the writer, we are basically just buffering one block of
 * data, not the whole file.
 *
 * <p>
 * The implementation used here is to look at the "string" being added to the writer and categorize it along two
 * dimensions:
 * <ul>
 * <li>Small vs large</li>
 * <li>Byte vs char</li>
 * </ul>
 *
 * These dimensions are broken out in the following way:
 * <li>Small byte "strings" are packed into a byte block, and we maintain a linked list of these byte blocks.</li>
 * <li>"Large" byte "strings" are stored directly, meaning a byte[] array is allocated for their data, then a reference
 * to that array is added to a byte-array block. (And again, we maintain a linked list of these byte-array blocks). It
 * is not typical for CSV data to contain a cell this large, but the feature is there for completeness. We do not want
 * want large "strings" to contaminate our packed byte blocks because they would not likely pack into them tightly (it
 * would become more likely to have allocated blocks with unused storage at the end, because the last big string
 * wouldn't fit in the current block). It's OK to keep them on their own because by definition, large "strings" are not
 * going to have much overhead, as a percentage of the size of their text content.</li>
 * </ul>
 */
public final class DenseStorageWriter {
    /**
     * The ints in this array indicate where the next item is stored:
     * <ul>
     * <li>{@link DenseStorageConstants#LARGE_BYTE_ARRAY_SENTINEL}:
     * {@link DenseStorageWriter#largeByteArrayWriter}.</li>
     * <li>&gt; 0: {@link DenseStorageWriter#byteWriter} (the number of chars is equal to this value)</li>
     * <li>== 0: no bytes, so they're not stored anywhere. Will be interpreted as a ByteSlice with arbitrary byte data
     * and length 0.</li>
     * </ul>
     */
    private final QueueWriter.IntWriter controlWriter;
    /**
     * Byte sequences < DENSE_THRESHOLD are compactly stored here
     */
    private final QueueWriter.ByteWriter byteWriter;
    /**
     * Byte sequences >= DENSE_THRESHOLD are stored here
     */
    private final QueueWriter.ByteArrayWriter largeByteArrayWriter;

    /**
     * Constructor
     */
    public DenseStorageWriter() {
        this.controlWriter = new QueueWriter.IntWriter(DenseStorageConstants.CONTROL_QUEUE_SIZE);
        this.byteWriter = new QueueWriter.ByteWriter(DenseStorageConstants.PACKED_QUEUE_SIZE);
        this.largeByteArrayWriter = new QueueWriter.ByteArrayWriter(DenseStorageConstants.ARRAY_QUEUE_SIZE);
    }

    public DenseStorageReader newReader() {
        return new DenseStorageReader(
                controlWriter.newReader(),
                byteWriter.newReader(),
                largeByteArrayWriter.newReader());
    }

    /**
     * Append a {@link ByteSlice} to the queue. The data will be diverted to one of the two specialized underlying
     * queues, depending on its size.
     */
    public void append(final ByteSlice bs) {
        final boolean fctrl;
        final int size = bs.size();
        if (size >= DenseStorageConstants.LARGE_THRESHOLD) {
            final byte[] data = new byte[size];
            bs.copyTo(data, 0);
            largeByteArrayWriter.addByteArray(data);
            fctrl = controlWriter.addInt(DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL);
        } else {
            byteWriter.addBytes(bs);
            fctrl = controlWriter.addInt(size);
        }
        // If the control queue flushed, then flush all the data queues, so the reader doesn't block for a long time
        // waiting for some unflushed data queue. One might worry this this is inefficient, but (a) it doesn't happen
        // very often and (b) in our queue code, partially-filled blocks can share non-overlapping parts of their
        // large underlying data array, so it's not too wasteful. Put another way, flushing an empty queue does nothing;
        // flushing a partially-filled queue allocates a new QueueNode but not a new underlying data array;
        // flushing a full queue will allocates a new QueueNode and (at the next write) a new underlying data array.
        if (fctrl) {
            byteWriter.flush();
            largeByteArrayWriter.flush();
        }
    }

    /**
     * Call this method to indicate when you are finished writing to the queue.
     */
    public void finish() {
        controlWriter.finish();
        byteWriter.finish();
        largeByteArrayWriter.finish();
    }
}
