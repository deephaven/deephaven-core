package io.deephaven.dbtypes;

import org.jetbrains.annotations.NotNull;

import javax.activation.MimetypesFileTypeMap;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * An image which can be persisted.
 */
public class DbImageImpl implements DbImage, Externalizable {

    private static final long serialVersionUID = 5618698544107854141L;
    private static final MimetypesFileTypeMap MIME_MAP = new MimetypesFileTypeMap();

    private String name;
    private String type;
    private byte[] bytes;
    private transient BufferedImage image;

    /**
     * This method is only present to support Externalizable. It should not be used otherwise.
     */
    public DbImageImpl() {
        this.name = null;
        this.type = null;
        this.bytes = null;
        this.image = null;
    }

    /**
     * Creates an image from image file bytes.
     *
     * @param bytes image file bytes
     * @throws NullPointerException if bytes is null.
     */
    public DbImageImpl(final byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException();
        }

        this.name = null;
        this.type = null;
        this.bytes = bytes;
        this.image = null;
    }

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    public DbImageImpl(final String file) throws IOException {
        this(new java.io.File(file));
    }

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    public DbImageImpl(final java.io.File file) throws IOException {
        this.name = file.getName();
        this.type = MIME_MAP.getContentType(file.getName());
        this.bytes = DbFileImpl.loadBytes(file);
        this.image = null;
    }

    /**
     * Creates an image from a <code>BufferedImage</code>.
     *
     * @param image input image
     * @throws NullPointerException image is null.
     */
    public DbImageImpl(final BufferedImage image) {
        if (image == null) {
            throw new NullPointerException();
        }

        this.bytes = null;
        this.image = image;
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
        if (bytes == null && image == null) {
            throw new IllegalStateException(
                    "Bytes and Image are both null.  The zero argument constructor should only be used during deserialization.");
        }

        if (bytes == null) {
            synchronized (this) {
                if (bytes == null) {
                    try {
                        bytes = image2Bytes(image, "JPEG");
                    } catch (IOException e) {
                        throw new UncheckedIOException("Problem loading bytes", e);
                    }
                }
            }
        }

        return bytes;
    }

    @Override
    public BufferedImage getBufferedImage() {
        if (bytes == null && image == null) {
            throw new IllegalStateException(
                    "Bytes and Image are both null.  The zero argument constructor should only be used during deserialization.");
        }

        if (image == null) {
            synchronized (this) {
                if (image == null) {
                    try {
                        this.image = bytes2Image(this.bytes);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Problem loading image", e);
                    }
                }
            }
        }

        return image;
    }

    @Override
    public int getHeight() {
        return getBufferedImage().getHeight();
    }

    @Override
    public int getWidth() {
        return getBufferedImage().getWidth();
    }

    @Override
    public Color getColor(final int x, final int y) {
        return new Color(getBufferedImage().getRGB(x, y));
    }

    @Override
    public int getRed(final int x, final int y) {
        return getColor(x, y).getRed();
    }

    @Override
    public int getGreen(final int x, final int y) {
        return getColor(x, y).getGreen();
    }

    @Override
    public int getBlue(final int x, final int y) {
        return getColor(x, y).getBlue();
    }

    @Override
    public int getGray(final int x, final int y) {
        final int r = getRed(x, y);
        final int g = getGreen(x, y);
        final int b = getBlue(x, y);

        return (r + g + b) / 3;
    }

    @Override
    public void write(String file) throws IOException {
        write("JPEG", file);
    }

    @Override
    public void write(final String formatName, final String file) throws IOException {
        if (!ImageIO.write(getBufferedImage(), formatName, new java.io.File(file))) {
            throw new IllegalArgumentException("No appropriate image writer found.  formatName=" + formatName);
        }
    }

    @Override
    public DbImageImpl subImage(final int x, final int y, final int w, final int h) {
        final BufferedImage img = getBufferedImage().getSubimage(x, y, w, h);
        return new DbImageImpl(img);
    }

    @Override
    public DbImageImpl resize(final int width, final int height, final ImageScalingAlgorithm algo) {
        java.awt.Image tmp = getBufferedImage().getScaledInstance(width, height,
                algo == null ? ImageScalingAlgorithm.DEFAULT.get() : algo.get());
        BufferedImage resized = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = resized.createGraphics();
        g2d.drawImage(tmp, 0, 0, null);
        g2d.dispose();
        return new DbImageImpl(resized);
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
        this.image = ImageIO.read(new ByteArrayInputStream(bytes));
    }


    public static class Factory implements DbImageFactory {

        @Override
        public DbImage newInstance(byte[] bytes) {
            try {
                return new DbImageImpl(bytes);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create image.", e);
            }
        }

        @Override
        public DbImage newInstance(String file) {
            try {
                return new DbImageImpl(file);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create image.", e);
            }
        }

        @Override
        public DbImage newInstance(File file) {
            try {
                return new DbImageImpl(file);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create image.", e);
            }
        }

        @Override
        public DbImage newInstance(BufferedImage image) {
            try {
                return new DbImageImpl(image);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create image.", e);
            }
        }
    }

    ////////////// Utility functions for dealing with images. //////////////


    /**
     * Converts a <code>BufferedImage</code> to a byte array.
     *
     * @param image input image.
     * @param formatName output format name (e.g. "JPEG")
     * @return byte array representation of the image. This is the same as the contents of an image file of the
     *         specified type.
     * @throws IOException problem loading the image.
     */
    static byte[] image2Bytes(@NotNull final BufferedImage image, final String formatName) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();

        if (!ImageIO.write(image, formatName, bos)) {
            throw new IllegalArgumentException("No appropriate image writer found.  formatName=" + formatName);
        }

        bos.close();
        return bos.toByteArray();
    }

    /**
     * Loads a <code>BufferedImage</code> from a byte array of the same format as an image file.
     *
     * @param bytes byte array representation of the image.
     * @return buffered image
     * @throws IOException problem loading the image.
     */
    static BufferedImage bytes2Image(final byte[] bytes) throws IOException {
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        final BufferedImage image = ImageIO.read(bis);
        bis.close();
        return image;
    }

}
