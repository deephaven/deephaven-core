package io.deephaven.dbtypes;

import java.io.IOException;

/**
 * An opaque array of bytes, intended to represent the contents of a file.
 */
public interface DbFile {

    /**
     * Gets the file name.
     *
     * @return file name.
     */
    String getName();

    /**
     * Gets the file type.
     *
     * @return file type.
     */
    String getType();

    /**
     * Gets the file content as a byte array.
     *
     * @return file content as a byte array.
     */
    byte[] getBytes();

    /**
     * Writes the file out.
     *
     * @param file output file name
     * @throws IOException problem writing out the file.
     */
    void write(final String file) throws IOException;

    /**
     * Creates an empty file.
     */
    static DbFile newInstance() {
        return FactoryInstances.getFileFactory().newInstance();
    }

    /**
     * Creates a file from file bytes.
     *
     * @param bytes file bytes
     * @throws NullPointerException if bytes is null.
     */
    static DbFile newInstance(final byte[] bytes) {
        return FactoryInstances.getFileFactory().newInstance(bytes);
    }

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    static DbFile newInstance(final String file) {
        return FactoryInstances.getFileFactory().newInstance(file);
    }

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    static DbFile newInstance(final java.io.File file) {
        return FactoryInstances.getFileFactory().newInstance(file);
    }
}
