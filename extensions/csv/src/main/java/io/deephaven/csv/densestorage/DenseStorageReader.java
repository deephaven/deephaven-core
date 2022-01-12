
package io.deephaven.csv.densestorage;

import io.deephaven.csv.containers.ByteSlice;
import io.deephaven.csv.util.CsvReaderException;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Companion to the {@link DenseStorageWriter}. See the documentation there for details.
 */
public final class DenseStorageReader {
    /**
     * Byte sequences < DENSE_THRESHOLD are compactly stored here
     */
    private final QueueReader.ByteReader byteReader;
    /**
     * Byte sequences >= DENSE_THRESHOLD are stored here
     */
    private final QueueReader.ByteArrayReader largeByteArrayReader;
    /**
     * Control bytes (lengths, negated lengths, or sentinels). See DenseStorageWriter.
     */
    private final QueueReader.IntReader controlReader;
    /**
     * For the "out" parameter of controlReader.tryGetInt()
     */
    private final MutableInt intHolder = new MutableInt();

    /**
     * Constructor.
     */
    public DenseStorageReader(final QueueReader.IntReader controlReader,
            final QueueReader.ByteReader byteReader,
            final QueueReader.ByteArrayReader largeByteArrayReader) {
        this.controlReader = controlReader;
        this.byteReader = byteReader;
        this.largeByteArrayReader = largeByteArrayReader;
    }

    /**
     * Tries to get the next slice from one of the inner QueueReaders. Uses data in the 'controlReader' to figure out
     * which QueueReader the next slice is coming from.
     * 
     * @param bs If the method returns true, the contents of this parameter will be updated.
     * @return true if there is more data, and the ByteSlice has been populated. Otherwise, false.
     */
    public boolean tryGetNextSlice(final ByteSlice bs)
            throws CsvReaderException {
        if (!controlReader.tryGetInt(intHolder)) {
            return false;
        }
        final int control = intHolder.intValue();
        if (control == DenseStorageConstants.LARGE_BYTE_ARRAY_SENTINEL) {
            mustSucceed(largeByteArrayReader.tryGetBytes(bs), "largeByteArrayReader");
            return true;
        }
        mustSucceed(byteReader.tryGetBytes(control, bs), "byteReader");
        return true;
    }

    /**
     * Convenience method that throws an exception if "success" is false.
     */
    private static void mustSucceed(final boolean success, final String what) throws CsvReaderException {
        if (!success) {
            throw new CsvReaderException("Data unexpectedly exhausted: " + what);
        }
    }
}
