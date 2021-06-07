package io.deephaven.dbtypes;

/**
 * A factory for creating <code>DbFile</code>s.
 */
public interface DbFileFactory {

    /**
     * Creates an empty file.
     */
    DbFile newInstance();

    /**
     * Creates a file from file bytes.
     *
     * @param bytes file bytes
     * @throws NullPointerException if bytes is null.
     */
    DbFile newInstance(final byte[] bytes);

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    DbFile newInstance(final String file);

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    DbFile newInstance(final java.io.File file);
}
