//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.BufferListener;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.DrainableColumn;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.FieldNodeListener;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The {@code SingleElementListHeaderWriter} is a specialized {@link DrainableColumn} implementation that writes the
 * header for singleton list-wrapped columns in Apache Arrow record batches.
 * <p>
 * This writer ensures compatibility with Apache Arrow's format by providing the necessary metadata and offsets for a
 * single-element list, while omitting unnecessary buffers such as validity buffers. It is designed to write the header
 * information for a column where all rows are represented as a singleton list, with no null values.
 *
 * @see SingleElementListHeaderReader
 */
public class SingleElementListHeaderWriter extends DrainableColumn {

    private final int numElements;

    public SingleElementListHeaderWriter(final int numElements) {
        this.numElements = numElements;
    }

    @Override
    public void visitFieldNodes(FieldNodeListener listener) {
        listener.noteLogicalFieldNode(1, 0);
    }

    @Override
    public void visitBuffers(BufferListener listener) {
        // no need to send any validity buffer since all elements are non-null
        listener.noteLogicalBuffer(0);

        // the start offset and end offset; note this is a multiple of 8
        listener.noteLogicalBuffer(Integer.BYTES * 2);
    }

    @Override
    public int nullCount() {
        return 0;
    }

    @Override
    public int drainTo(final OutputStream outputStream) throws IOException {
        // allow this input stream to be re-read
        final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

        // write offsets array
        dos.writeInt(0);
        dos.writeInt(numElements);
        dos.flush();

        // bytes written is a multiple of 8; no need to pad
        return 2 * Integer.BYTES;
    }

    @Override
    public int available() throws IOException {
        return 2 * Integer.BYTES;
    }
}
