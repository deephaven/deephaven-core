package io.deephaven.dbtypes;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * An image which can be persisted.
 */
public interface DbImage extends DbFile {

    /**
     * Algorithms for scaling images.
     */
    enum ImageScalingAlgorithm {
        /**
         * Default image-scaling algorithm.
         */
        DEFAULT(java.awt.Image.SCALE_DEFAULT),

        /**
         * Image-scaling algorithm that gives higher priority to scaling speed than smoothness of
         * the scaled image.
         */
        FAST(java.awt.Image.SCALE_FAST),

        /**
         * Image-scaling algorithm that gives higher priority to image smoothness than scaling
         * speed.
         */
        SMOOTH(java.awt.Image.SCALE_SMOOTH),

        /**
         * Image scaling algorithm embodied in the <code>ReplicateScaleFilter</code> class. The
         * <code>Image</code> object is free to substitute a different filter that performs the same
         * algorithm yet integrates more efficiently into the imaging infrastructure supplied by the
         * toolkit.
         *
         * @see java.awt.image.ReplicateScaleFilter
         */
        REPLICATE(java.awt.Image.SCALE_REPLICATE),

        /**
         * Area Averaging image scaling algorithm. The image object is free to substitute a
         * different filter that performs the same algorithm yet integrates more efficiently into
         * the image infrastructure supplied by the toolkit.
         *
         * @see java.awt.image.AreaAveragingScaleFilter
         */
        AREA_AVERAGING(java.awt.Image.SCALE_AREA_AVERAGING);

        private final int val;

        ImageScalingAlgorithm(int val) {
            this.val = val;
        }

        /**
         * Gets the AWT integer representing the scaling algorithm.
         *
         * @return AWT integer representing the scaling algorithm.
         */
        int get() {
            return val;
        }
    }

    /**
     * Gets the image file content as a byte array.
     *
     * @return image file content as a byte array.
     */
    byte[] getBytes();

    /**
     * Gets a <code>BufferedImage</code> representation of this image.
     *
     * @return <code>BufferedImage</code> representation of this image.
     */
    BufferedImage getBufferedImage();

    /**
     * Gets the height of the image in pixels.
     *
     * @return height of the image in pixels.
     */
    int getHeight();

    /**
     * Gets the width of the image in pixels.
     *
     * @return width of the image in pixels.
     */
    int getWidth();

    /**
     * Gets the color of the pixel located at (x,y).
     *
     * @param x x-location in pixels.
     * @param y y-location in pixels.
     * @return color of the pixel located at (x,y).
     */
    Color getColor(final int x, final int y);

    /**
     * Gets the red component in the range 0-255 in the default sRGB space for the pixel located at
     * (x,y).
     *
     * @param x x-location in pixels.
     * @param y y-location in pixels.
     * @return red color component of the pixel located at (x,y).
     */
    int getRed(final int x, final int y);

    /**
     * Gets the green component in the range 0-255 in the default sRGB space for the pixel located
     * at (x,y).
     *
     * @param x x-location in pixels.
     * @param y y-location in pixels.
     * @return green color component of the pixel located at (x,y).
     */
    int getGreen(final int x, final int y);

    /**
     * Gets the blue component in the range 0-255 in the default sRGB space for the pixel located at
     * (x,y).
     *
     * @param x x-location in pixels.
     * @param y y-location in pixels.
     * @return blue color component of the pixel located at (x,y).
     */
    int getBlue(final int x, final int y);

    /**
     * Gets the gray-scale value in the range 0-255 in the default sRGB space for the pixel located
     * at (x,y).
     *
     * @param x x-location in pixels.
     * @param y y-location in pixels.
     * @return gray-scale value of the pixel located at (x,y).
     */
    int getGray(final int x, final int y);

    /**
     * Writes the image to file.
     *
     * @param formatName output format name (e.g. "JPEG")
     * @param file output file
     * @throws IOException problem writing out the image.
     */
    void write(final String formatName, final String file) throws IOException;

    /**
     * Creates a sub-image of this image.
     *
     * @param x the X coordinate of the upper-left corner of the specified rectangular region
     * @param y the Y coordinate of the upper-left corner of the specified rectangular region
     * @param w the width of the specified rectangular region
     * @param h the height of the specified rectangular region
     * @return sub-image
     */
    DbImage subImage(final int x, final int y, final int w, final int h);

    /**
     * Creates a new resized image.
     *
     * @param width new image width in pixels.
     * @param height new image height in pixels.
     * @return resized image.
     */
    default DbImage resize(final int width, final int height) {
        return resize(width, height, null);
    }

    /**
     * Creates a new resized image.
     *
     * @param width new image width in pixels.
     * @param height new image height in pixels.
     * @param algo algorithm used to rescale the image. <code>null</code> causes the default to be
     *        usedl.
     * @return resized image.
     */
    DbImage resize(final int width, final int height, final ImageScalingAlgorithm algo);

    /**
     * Creates an image from image file bytes.
     *
     * @param bytes image file bytes
     * @throws NullPointerException if bytes is null.
     */
    static DbImage newInstance(final byte[] bytes) {
        return FactoryInstances.getImageFactory().newInstance(bytes);
    }

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    static DbImage newInstance(final String file) {
        return FactoryInstances.getImageFactory().newInstance(file);
    }

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    static DbImage newInstance(final java.io.File file) {
        return FactoryInstances.getImageFactory().newInstance(file);
    }

    /**
     * Creates an image from a <code>BufferedImage</code>.
     *
     * @param image input image
     * @throws NullPointerException image is null.
     */
    static DbImage newInstance(final BufferedImage image) {
        return FactoryInstances.getImageFactory().newInstance(image);
    }
}
