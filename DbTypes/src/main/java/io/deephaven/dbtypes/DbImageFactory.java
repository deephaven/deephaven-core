package io.deephaven.dbtypes;

import java.awt.image.BufferedImage;

/**
 * A factory for creating <code>DbImage</code>s.
 */
public interface DbImageFactory {

    /**
     * Creates an image from image file bytes.
     *
     * @param bytes image file bytes
     * @throws NullPointerException if bytes is null.
     */
    DbImage newInstance(final byte[] bytes);

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    DbImage newInstance(final String file);

    /**
     * Creates an image from an image file.
     *
     * @param file file location
     * @throws NullPointerException file is null.
     */
    DbImage newInstance(final java.io.File file);

    /**
     * Creates an image from a <code>BufferedImage</code>.
     *
     * @param image input image
     * @throws NullPointerException image is null.
     */
    DbImage newInstance(final BufferedImage image);
}
