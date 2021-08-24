/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A font used to represent text.
 */
@SuppressWarnings("WeakerAccess")
public class Font implements Serializable {

    private static final long serialVersionUID = -524274466614096214L;


    /**
     * Style of font.
     */
    public enum FontStyle {
        /**
         * Plain text.
         */
        PLAIN(java.awt.Font.PLAIN),

        /**
         * Bold text
         */
        BOLD(java.awt.Font.BOLD),

        /**
         * Italic text
         */
        ITALIC(java.awt.Font.ITALIC),

        /**
         * Bold and italic text
         */
        BOLD_ITALIC(java.awt.Font.BOLD | java.awt.Font.ITALIC);

        private final int mask;

        FontStyle(int mask) {
            this.mask = mask;
        }

        /**
         * Gets the mask of this Style.
         *
         * @return int mask representing this Style
         */
        public int mask() {
            return mask;
        }

    }

    private static final Map<String, FontStyle> extraFontStyles = new LinkedHashMap<>();

    static {
        extraFontStyles.put("P", FontStyle.PLAIN);
        extraFontStyles.put("B", FontStyle.BOLD);
        extraFontStyles.put("I", FontStyle.ITALIC);
        extraFontStyles.put("BI", FontStyle.BOLD_ITALIC);
        extraFontStyles.put("IB", FontStyle.BOLD_ITALIC);
    }

    /**
     * Returns a font style.
     *
     * @param style case insensitive font style descriptor
     * @throws IllegalArgumentException {@code style} must not be null
     * @return FontStyle corresponding to {@code style}
     */
    public static FontStyle fontStyle(String style) {
        if (style == null) {
            throw new IllegalArgumentException("Style can not be null");
        }

        style = style.toUpperCase().trim();

        try {
            return FontStyle.valueOf(style);
        } catch (Exception ignored) {
            // pass
        }

        final FontStyle fontStyle = extraFontStyles.get(style);

        if (fontStyle != null) {
            return fontStyle;
        } else {
            throw new UnsupportedOperationException("FontStyle " + style + " not defined");
        }
    }

    /**
     * Returns the names of available font styles.
     *
     * @return array of available FontStyle names
     */
    public static String[] fontStyleNames() {
        final ArrayList<String> names = new ArrayList<>();
        Arrays.stream(FontStyle.values()).forEach(x -> names.add(x.name()));
        names.addAll(extraFontStyles.keySet());
        return names.stream().toArray(String[]::new);
    }



    ////////////////////////// Font //////////////////////////


    private final String family;
    private final FontStyle style;
    private final int size;

    private final java.awt.Font font;

    /**
     * Creates a new instance of the Font with the specified {@code family}, {@code style}, and
     * {@code size}.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     */
    public Font(final String family, final FontStyle style, final int size) {
        this.family = family == null ? "Arial" : family;
        this.style = style == null ? FontStyle.PLAIN : style;
        this.size = size;

        // noinspection MagicConstant
        this.font = new java.awt.Font(this.family, this.style.mask(), this.size);
    }

    /**
     * Creates a new instance of the Font with the specified {@code family}, {@code style}, and
     * {@code size}.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     */
    public Font(final String family, final String style, final int size) {
        this(family, fontStyle(style), size);
    }


    ////////////////////////// internal functionality //////////////////////////


    @Override
    public String toString() {
        return "Font{" + family + "," + style + "," + size + "}";
    }


    ////////////////////////// static helpers //////////////////////////


    /**
     * Returns a font.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return font with the specified family, style and size
     */
    public static Font font(final String family, final FontStyle style, final int size) {
        return new Font(family, style, size);
    }

    /**
     * Returns a font.
     *
     * @param family font family; if null, set to Arial
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @param size the point size of the Font
     * @return font with the specified family, style and size
     */
    public static Font font(final String family, final String style, final int size) {
        return new Font(family, style, size);
    }

    /**
     * Returns the names of available Font families.
     *
     * @return array of available Font family names
     */
    public static String[] fontFamilyNames() {
        return java.awt.GraphicsEnvironment.getLocalGraphicsEnvironment()
            .getAvailableFontFamilyNames();
    }


    ////////////////////////// font transforms //////////////////////////


    /**
     * Returns an instance of this Font with the family changed to the specified {@code family}
     *
     * @param family font family; if null, set to Arial
     * @return Font with {@code family} and this Font's style and size
     */
    public Font refamily(final String family) {
        return new Font(family, this.style, this.size);
    }

    /**
     * Gets an instance of this Font with the style changed to the specified {@code style}
     *
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @return Font with {@code style} and this Font's family and size
     */
    @SuppressWarnings("unused")
    public Font restyle(final FontStyle style) {
        return new Font(this.family, style, this.size);
    }

    /**
     * Returns an instance of this Font with the style changed to the specified {@code style}
     *
     * @param style font style; if null, set to {@link Font.FontStyle} PLAIN
     * @return Font with {@code style} and this Font's family and size
     */
    public Font restyle(final String style) {
        return new Font(this.family, style, this.size);
    }

    /**
     * Returns an instance of this Font with the size changed to the specified {@code size}
     *
     * @param size point size of the font
     * @return Font with {@code size} and this Font's family and style
     */
    public Font resize(final int size) {
        return new Font(this.family, this.style, size);
    }


    ////////////////////////// Font Representations //////////////////////////


    /**
     * Returns the Java object representative of this Font.
     *
     * @return Java object representative of this Font
     */
    public java.awt.Font javaFont() {
        return font;
    }


    ///////////////////////// Object /////////////////////////

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Font && ((Font) obj).font.equals(this.font);
    }

    @Override
    public int hashCode() {
        return font.hashCode();
    }
}
