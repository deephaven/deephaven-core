//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.modelfarm;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;

/**
 * Serialize model inputs to a file.
 *
 * @param <T> input data type
 */
public class ModelInputSerializer<T> implements Model<T>, AutoCloseable {

    private final FileOutputStream fos;
    private final ObjectOutputStream oos;
    private boolean isClosed = false;

    /**
     * Creates a new serializer.
     *
     * @param filename output file name
     */
    public ModelInputSerializer(final String filename) {
        try {
            this.fos = new FileOutputStream(filename);
            this.oos = new ObjectOutputStream(this.fos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void exec(T data) {
        if (!isClosed) {
            try {
                oos.writeObject(data);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Closes the output stream.
     *
     * @throws IOException problem closing the output stream
     */
    public synchronized void close() throws IOException {
        oos.close();
        fos.close();
        isClosed = true;
    }
}
