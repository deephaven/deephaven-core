package io.deephaven.dbtypes;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

public class DbImageImplTest extends BaseArrayTestCase {

    final private String file = Configuration.getInstance().getDevRootPath()
            + "/DbTypesImpl/src/test/resources/io/deephaven/dbtypes/white.jpg";

    private void assertEquals(DbImage image1, DbImage image2) {
        assertEquals(image1.getName(), image2.getName());
        assertEquals(image1.getType(), image2.getType());
        assertEquals(image1.getBytes(), image2.getBytes());
        assertEquals(1, image2.getHeight());
        assertEquals(1, image2.getWidth());
    }

    private DbImageImpl[] loadImages() throws IOException {
        return new DbImageImpl[] {
                new DbImageImpl(file),
                new DbImageImpl(new java.io.File(file)),
                new DbImageImpl(DbFileImpl.loadBytes(new java.io.File(file))),
                new DbImageImpl(ImageIO.read(new java.io.File(file)))
        };
    }

    public void testNullConstructors() throws IOException {
        try {
            new DbImageImpl((byte[]) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }

        try {
            new DbImageImpl((String) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }

        try {
            new DbImageImpl((java.io.File) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }

        try {
            new DbImageImpl((BufferedImage) null);
            fail("Should have thrown an exception.");
        } catch (NullPointerException e) {
            // pass
        }
    }

    public void testNullConstructor() {
        final DbImageImpl img = new DbImageImpl();
        assertEquals(null, img.getName());
        assertEquals(null, img.getType());

        try {
            img.getBytes();
            fail("Should have thrown an exception.");
        } catch (IllegalStateException e) {
            // pass
        }

        try {
            img.getBufferedImage();
            fail("Should have thrown an exception.");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    public void testGetName() throws IOException {
        final DbImageImpl[] images = loadImages();

        assertEquals("white.jpg", images[0].getName());
        assertEquals("white.jpg", images[1].getName());
        assertEquals(null, images[2].getName());
        assertEquals(null, images[3].getName());
    }

    public void testGetType() throws IOException {
        final DbImageImpl[] images = loadImages();

        assertEquals("image/jpeg", images[0].getType());
        assertEquals("image/jpeg", images[1].getType());
        assertEquals(null, images[2].getType());
        assertEquals(null, images[3].getType());
    }

    public void testGetBytes() throws IOException {
        final DbImageImpl[] images = loadImages();
        final byte[] bytes = DbFileImpl.loadBytes(new java.io.File(file));

        for (int i = 0; i < 3; i++) {
            final DbImageImpl image = images[i];
            assertEquals(bytes, image.getBytes());
            i++;
        }
    }

    public void testGetBufferedImage() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertNotNull(image.getBufferedImage());
        }
    }

    public void testGetHeight() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(1, image.getHeight());
        }
    }

    public void testGetWidth() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(1, image.getWidth());
        }
    }

    public void testGetColor() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(new Color(255, 255, 255), image.getColor(0, 0));
        }
    }

    public void testGetRed() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(255, image.getRed(0, 0));
        }
    }

    public void testGetGreen() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(255, image.getGreen(0, 0));
        }
    }

    public void testGetBlue() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(255, image.getBlue(0, 0));
        }
    }

    public void testGetGray() throws IOException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            assertEquals(255, image.getGray(0, 0));
        }
    }

    public void testSerialization() throws IOException, ClassNotFoundException {
        final DbImageImpl[] images = loadImages();

        for (DbImageImpl image : images) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(image);
            oos.close();
            final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            final DbImageImpl img = (DbImageImpl) ois.readObject();
            assertNotNull(img);
            assertEquals(image, img);
        }
    }

    public void testSubImage() throws IOException {
        final DbImageImpl[] images = loadImages();

        final int w = 1;
        final int h = 1;

        for (DbImageImpl image : images) {
            final DbImageImpl img = image.subImage(0, 0, w, h);
            assertEquals(h, img.getHeight());
            assertEquals(w, img.getWidth());
        }
    }

    public void testResize() throws IOException {
        final DbImageImpl[] images = loadImages();

        final int w = 100;
        final int h = 200;

        for (DbImageImpl image : images) {
            final DbImage img = image.resize(w, h);
            assertEquals(h, img.getHeight());
            assertEquals(w, img.getWidth());
        }
    }

    public void testWrite1() throws IOException {
        final DbImageImpl[] images = loadImages();
        final String filename = Configuration.getInstance().getTempPath("dbimagetest_file.jpg");

        for (DbImageImpl image : images) {
            image.write(filename);
            final DbImageImpl img = new DbImageImpl(filename);
            assertEquals(1, img.getHeight());
            assertEquals(1, img.getWidth());
        }
    }

    public void testWrite2() throws IOException {
        final DbImageImpl[] images = loadImages();
        final String filename = Configuration.getInstance().getTempPath("dbimagetest_file.jpg");

        for (DbImageImpl image : images) {
            image.write("JPG", filename);
            final DbImageImpl img = new DbImageImpl(filename);
            assertEquals(1, img.getHeight());
            assertEquals(1, img.getWidth());

            try {
                image.write("JUNK", filename);
                fail("Should have thrown an exception");
            } catch (IllegalArgumentException e) {
                // pass
            }
        }
    }

    public void testLoadBytesFile() throws IOException {
        final byte[] b = DbFileImpl.loadBytes(new java.io.File(file));
        final BufferedImage image = ImageIO.read(new ByteArrayInputStream(b));
        assertNotNull(image);
    }

    public void testImage2Bytes() throws IOException {
        final BufferedImage image = ImageIO.read(new java.io.File(file));
        final byte[] b = DbImageImpl.image2Bytes(image, "JPEG");
        final BufferedImage image2 = ImageIO.read(new ByteArrayInputStream(b));
        assertNotNull(image2);

        try {
            DbImageImpl.image2Bytes(image, "JUNK");
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    public void testLoadImage() throws IOException {
        final byte[] b = DbFileImpl.loadBytes(new java.io.File(file));
        final BufferedImage image = DbImageImpl.bytes2Image(b);
        assertNotNull(image);
    }

}
