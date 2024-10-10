//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.BufferListener;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.DrainableColumn;
import io.deephaven.extensions.barrage.chunk.ChunkWriter.FieldNodeListener;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This helper class is used to generate only the header of an arrow list that contains a single element.
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
