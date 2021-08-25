package io.deephaven.dbtypes;

import io.deephaven.base.verify.Require;
import org.apache.commons.io.IOUtils;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;

/**
 * An opaque array of bytes, intended to represent the contents of a file.
 */
public class DbFileImpl implements DbFile, Externalizable {

    private static final long serialVersionUID = 5618698544107854141L;
    private static final MimetypesFileTypeMap MIME_MAP = new MimetypesFileTypeMap();

    private String name;
    private String type;
    private byte[] bytes;

    /**
     * Creates an empty file.
     */
    public DbFileImpl() {
        this.name = null;
        this.type = null;
        this.bytes = null;
    }

    /**
     * Creates a file from file bytes.
     *
     * @param bytes file bytes
     * @throws NullPointerException if bytes is null.
     */
    public DbFileImpl(final byte[] bytes) {
        this.name = null;
        this.type = null;
        this.bytes = Require.neqNull(bytes, "bytes");
    }

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    public DbFileImpl(final String file) throws IOException {
        this(new java.io.File(file));
    }

    /**
     * Creates a file from a file on the filesystem.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    public DbFileImpl(final java.io.File file) throws IOException {
        this.name = file.getName();
        this.type = MIME_MAP.getContentType(file.getName());
        this.bytes = loadBytes(file);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public void write(final String file) throws IOException {
        writeBytes(bytes, new java.io.File(file));
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeObject(name);
        out.writeObject(type);
        out.writeObject(getBytes());
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        this.name = (String) in.readObject();
        this.type = (String) in.readObject();
        this.bytes = (byte[]) in.readObject();
    }

    /**
     * A factory for creating <code>DbFile</code>s.
     */
    public static class Factory implements DbFileFactory {

        @Override
        public DbFile newInstance() {
            try {
                return new DbFileImpl();
            } catch (Exception e) {
                throw new RuntimeException("Unable to create file.", e);
            }
        }

        @Override
        public DbFile newInstance(byte[] bytes) {
            try {
                return new DbFileImpl(bytes);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create file.", e);
            }
        }

        @Override
        public DbFile newInstance(String file) {
            try {
                return new DbFileImpl(file);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create file.", e);
            }
        }

        @Override
        public DbFile newInstance(File file) {
            try {
                return new DbFileImpl(file);
            } catch (IOException e) {
                throw new RuntimeException("Unable to create file.", e);
            }
        }
    }

    ////////////// Utility functions for dealing with files. //////////////


    /**
     * Reads a file in as a byte array.
     *
     * @param file file to load.
     * @return contents of the file.
     * @throws IOException problem loading the file.
     */
    static byte[] loadBytes(final java.io.File file) throws IOException {
        final FileInputStream fis = new FileInputStream(file);
        final byte[] bytes = IOUtils.toByteArray(fis);
        fis.close();
        return bytes;
    }

    /**
     * Writes a byte array to a file.
     *
     * @param bytes bytes to write out.
     * @param file file to write to.
     * @throws IOException problem writing the file.
     */
    static void writeBytes(final byte[] bytes, final java.io.File file) throws IOException {
        final FileOutputStream fos = new FileOutputStream(file);
        fos.write(bytes);
        fos.close();
    }

}
