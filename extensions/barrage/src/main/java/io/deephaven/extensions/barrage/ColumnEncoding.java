//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import java.util.Objects;

/**
 * Arrow encoding to apply to a column when generating a Barrage schema. Auto-detected per column by
 * {@link io.deephaven.extensions.barrage.util.BarrageUtil#schemaFromTable} when structural guarantees make a particular
 * encoding clearly beneficial, or specified explicitly via
 * {@link io.deephaven.engine.table.Table#BARRAGE_SCHEMA_ATTRIBUTE}.
 *
 * <p>
 * An encoding carries two independent facets: an optional run-end encoding (REE) run_ends index width, and an optional
 * dictionary-index width. When both are present the column is doubly-encoded as {@code RunEndEncoded<Dictionary<...>>}
 * -- REE collapses runs on the row axis and the dictionary dedupes the per-run values. The historical single-facet
 * constants ({@code RUN_END_ENCODED_INT32}, {@code DICTIONARY_ENCODED_INT32}, ...) remain available as canonical
 * singletons; compose them with {@link #withDictionary} / {@link #withRunEnd}.
 */
public final class ColumnEncoding {

    /** Width of the REE {@code run_ends} index child. */
    public enum RunEndWidth {
        INT16, INT32, INT64;

        /** Maps an Arrow integer bit width to the corresponding run-end width. */
        public static RunEndWidth fromBitWidth(final int bitWidth) {
            return switch (bitWidth) {
                case 16 -> INT16;
                case 32 -> INT32;
                case 64 -> INT64;
                default -> throw new IllegalArgumentException("Unrecognized run-end encoded bit width: " + bitWidth);
            };
        }
    }

    /** Width of the dictionary index (the values shipped in the RecordBatch). */
    public enum DictWidth {
        INT8, INT16, INT32, INT64;

        /** Maps an Arrow integer bit width to the corresponding dictionary index width. */
        public static DictWidth fromBitWidth(final int bitWidth) {
            return switch (bitWidth) {
                case 8 -> INT8;
                case 16 -> INT16;
                case 32 -> INT32;
                case 64 -> INT64;
                default -> throw new IllegalArgumentException("Unrecognized dictionary encoded bit width: " + bitWidth);
            };
        }
    }

    // Canonical single-facet singletons. Int32 is the default width in each family when unspecified.
    public static final ColumnEncoding RUN_END_ENCODED_INT16 = new ColumnEncoding(RunEndWidth.INT16, null);
    public static final ColumnEncoding RUN_END_ENCODED_INT32 = new ColumnEncoding(RunEndWidth.INT32, null);
    public static final ColumnEncoding RUN_END_ENCODED_INT64 = new ColumnEncoding(RunEndWidth.INT64, null);
    public static final ColumnEncoding DICTIONARY_ENCODED_INT8 = new ColumnEncoding(null, DictWidth.INT8);
    public static final ColumnEncoding DICTIONARY_ENCODED_INT16 = new ColumnEncoding(null, DictWidth.INT16);
    public static final ColumnEncoding DICTIONARY_ENCODED_INT32 = new ColumnEncoding(null, DictWidth.INT32);
    public static final ColumnEncoding DICTIONARY_ENCODED_INT64 = new ColumnEncoding(null, DictWidth.INT64);

    /** {@code null} when the column is not run-end encoded. */
    private final RunEndWidth runEndWidth;
    /** {@code null} when the column is not dictionary encoded. */
    private final DictWidth dictWidth;

    private ColumnEncoding(final RunEndWidth runEndWidth, final DictWidth dictWidth) {
        this.runEndWidth = runEndWidth;
        this.dictWidth = dictWidth;
    }

    /**
     * Returns an encoding with the given (possibly {@code null}) facets. Prefer the named singletons and
     * {@link #withDictionary} / {@link #withRunEnd} for readability.
     */
    public static ColumnEncoding of(final RunEndWidth runEndWidth, final DictWidth dictWidth) {
        return new ColumnEncoding(runEndWidth, dictWidth);
    }

    /** Returns a copy of this encoding with dictionary encoding added (or its width replaced). */
    public ColumnEncoding withDictionary(final DictWidth width) {
        return new ColumnEncoding(runEndWidth, width);
    }

    /** Returns a copy of this encoding with run-end encoding added (or its width replaced). */
    public ColumnEncoding withRunEnd(final RunEndWidth width) {
        return new ColumnEncoding(width, dictWidth);
    }

    /** Returns {@code true} if this encoding includes run-end encoding. */
    public boolean isRunEndEncoded() {
        return runEndWidth != null;
    }

    /** Returns {@code true} if this encoding includes dictionary encoding. */
    public boolean isDictionaryEncoded() {
        return dictWidth != null;
    }

    /** The REE run_ends width, or {@code null} if not run-end encoded. */
    public RunEndWidth runEndWidth() {
        return runEndWidth;
    }

    /** The dictionary index width, or {@code null} if not dictionary encoded. */
    public DictWidth dictWidth() {
        return dictWidth;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnEncoding)) {
            return false;
        }
        final ColumnEncoding other = (ColumnEncoding) o;
        return runEndWidth == other.runEndWidth && dictWidth == other.dictWidth;
    }

    @Override
    public int hashCode() {
        return Objects.hash(runEndWidth, dictWidth);
    }

    @Override
    public String toString() {
        return "ColumnEncoding{runEnd=" + runEndWidth + ", dict=" + dictWidth + '}';
    }
}
