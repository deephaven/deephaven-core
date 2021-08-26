package io.deephaven.dbtypes;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.configuration.Configuration;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

public class DbFileImplTest extends BaseArrayTestCase {

    final private String file = Configuration.getInstance().getDevRootPath()
        + "/DbTypesImpl/src/test/resources/io/deephaven/dbtypes/white.jpg";
    final private String tempdir = Configuration.getInstance().getTempPath("DBFiletest");

    public void testNullConstructors() throws IOException {
        try {
            new DbFileImpl((byte[]) null);
            fail("Should have thrown an exception.");
        } catch (RequirementFailure e) {
            // pass
        }

        try {
            new DbFileImpl((String) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }

        try {
            new DbFileImpl((java.io.File) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }
    }

    public void testNullConstructor() {
        final DbFileImpl img = new DbFileImpl();
        assertEquals(null, img.getName());
        assertEquals(null, img.getType());
        assertEquals(null, img.getBytes());
    }

    public void testBytesConstructor() throws IOException {
        final byte[] bytes = DbFileImpl.loadBytes(new java.io.File(file));
        final DbFile f = new DbFileImpl(bytes);
        assertEquals(null, f.getName());
        assertEquals(null, f.getType());
        assertEquals(bytes, f.getBytes());
    }

    public void testStringConstructor() throws IOException {
        final byte[] bytes = DbFileImpl.loadBytes(new java.io.File(file));
        final DbFile f = new DbFileImpl(file);
        assertEquals("white.jpg", f.getName());
        assertEquals("image/jpeg", f.getType());
        assertEquals(bytes, f.getBytes());
    }

    public void testFileConstructor() throws IOException {
        final byte[] bytes = DbFileImpl.loadBytes(new java.io.File(file));
        final DbFile f = new DbFileImpl(new java.io.File(file));
        assertEquals("white.jpg", f.getName());
        assertEquals("image/jpeg", f.getType());
        assertEquals(bytes, f.getBytes());
    }

    public void testGetBytes() throws IOException {
        final byte[] bytes = DbFileImpl.loadBytes(new java.io.File(file));
        final DbFile f = new DbFileImpl(file);
        assertEquals(bytes, f.getBytes());
    }

    public void testSerialization() throws IOException, ClassNotFoundException {
        final DbFileImpl f = new DbFileImpl(file);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(f);
        oos.close();
        final ObjectInputStream ois =
            new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        final DbFileImpl fout = (DbFileImpl) ois.readObject();
        assertEquals(f.getName(), fout.getName());
        assertEquals(f.getType(), fout.getType());
        assertEquals(f.getBytes(), fout.getBytes());
    }

    public void testWrite() throws IOException {
        final String filename = tempdir + "/DBFiletest_file.jpg";
        final DbFileImpl f = new DbFileImpl(file);
        f.write(filename);
        final DbFileImpl fout = new DbFileImpl(filename);
        assertEquals(f.getBytes(), fout.getBytes());
    }

    public void testLoadBytesFile() throws IOException {
        final byte[] b = DbFileImpl.loadBytes(new java.io.File(file));
        final BufferedImage image = ImageIO.read(new ByteArrayInputStream(b));
        assertNotNull(image);
    }

    public void testWriteBytesFile() throws IOException {
        final String filename = tempdir + "/DBFiletest_file.jpg";
        final byte[] b = DbFileImpl.loadBytes(new java.io.File(file));
        DbFileImpl.writeBytes(b, new java.io.File(filename));
        final byte[] bout = DbFileImpl.loadBytes(new java.io.File(filename));
        assertEquals(b, bout);
    }

}
