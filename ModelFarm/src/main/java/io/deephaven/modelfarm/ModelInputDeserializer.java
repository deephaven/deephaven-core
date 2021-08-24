/*
 * Copyright (c) 2018. Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Deserialize model inputs to a file.
 *
 * @param <T> input data type
 */
public class ModelInputDeserializer<T> implements AutoCloseable {

    private final Class<T> c;
    private final FileInputStream fis;
    private final ObjectInputStream ois;
    private boolean isClosed = false;

    /**
     * Creates a new deserializer.
     *
     * @param c data type
     * @param filename input file name
     * @throws IOException problem creating input stream
     */
    public ModelInputDeserializer(final Class<T> c, final String filename) throws IOException {
        this.c = c;
        this.fis = new FileInputStream(filename);
        this.ois = new ObjectInputStream(this.fis);
    }

    /**
     * Returns the next input.
     *
     * @return next input
     * @throws IOException problem reading the next input
     * @throws ClassNotFoundException problem casting the object
     */
    public synchronized T next() throws IOException, ClassNotFoundException {
        if (isClosed) {
            throw new RuntimeException(
                "Attempting to access the next value after the stream is closed.");
        }

        return c.cast(ois.readObject());
    }

    /**
     * Closes the input stream.
     *
     * @throws IOException problem closing the input stream
     */
    public synchronized void close() throws IOException {
        this.ois.close();
        this.fis.close();
        isClosed = true;
    }

}
