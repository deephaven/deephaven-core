/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Utilities to support BigDecimal exhaust.
 *
 * Parquet and Avro decimal types make a whole column decimal type have a fixed precision and scale; BigDecimal columns
 * in Deephaven are, each value, arbitrary precision (its own precision and scale).
 *
 * For static tables, it is possible to compute overall precision and scale values that fit every existing value. For
 * refreshing tables, we need the user to tell us.
 */
public class BigDecimalUtils {
    public static final int INVALID_PRECISION_OR_SCALE = -1;

    /**
     * Immutable way to store and pass precision and scale values.
     */
    public static class PrecisionAndScale {
        public final int precision;
        public final int scale;

        public PrecisionAndScale(final int precision, final int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }

    /**
     * Compute an overall precision and scale that would fit all existing values in a table.
     *
     * @param t a Deephaven table
     * @param colName a Column for {@code t}, which should be of {@code BigDecimal} type
     * @return a {@code PrecisionAndScale} object result.
     */
    public static PrecisionAndScale computePrecisionAndScale(
            final Table t, final String colName) {
        final ColumnSource<BigDecimal> src = t.getColumnSource(colName, BigDecimal.class);
        return computePrecisionAndScale(t.getRowSet(), src);
    }

    /**
     * Compute an overall precision and scale that would fit all existing values in a column source.
     *
     * @param rowSet The rowset for the provided column
     * @param source a {@code ColumnSource} of {@code BigDecimal} type
     * @return a {@code PrecisionAndScale} object result.
     */
    public static PrecisionAndScale computePrecisionAndScale(
            final RowSet rowSet,
            final ColumnSource<BigDecimal> source) {
        final int sz = 4096;
        // we first compute max(precision - scale) and max(scale), which corresponds to
        // max(digits left of the decimal point), max(digits right of the decimal point).
        // Then we convert to (precision, scale) before returning.
        int maxPrecisionMinusScale = 0;
        int maxScale = 0;
        try (final ChunkSource.GetContext context = source.makeGetContext(sz);
                final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            final RowSequence rowSeq = it.getNextRowSequenceWithLength(sz);
            final ObjectChunk<BigDecimal, ? extends Values> chunk = source.getChunk(context, rowSeq).asObjectChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                final BigDecimal x = chunk.get(i);
                final int precision = x.precision();
                final int scale = x.scale();
                final int precisionMinusScale = precision - scale;
                if (precisionMinusScale > maxPrecisionMinusScale) {
                    maxPrecisionMinusScale = precisionMinusScale;
                }
                if (scale > maxScale) {
                    maxScale = scale;
                }
            }
        }
        return new PrecisionAndScale(maxPrecisionMinusScale + maxScale, maxScale);
    }

    /**
     * Immutable way to store and pass properties to get precision and scale for a given named column.
     */
    public static class PropertyNames {
        public final String columnName;
        public final String precisionProperty;
        public final String scaleProperty;

        public PropertyNames(final String columnName) {
            this.columnName = columnName;
            precisionProperty = columnName + ".precision";
            scaleProperty = columnName + ".scale";
        }
    }

    private static int getPrecisionAndScaleFromColumnProperties(
            final String columnName,
            final String property,
            final Properties columnProperties,
            final boolean allowNulls) {
        if (columnProperties == null) {
            return INVALID_PRECISION_OR_SCALE;
        }
        final String propertyValue = columnProperties.getProperty(property);
        if (propertyValue == null) {
            if (!allowNulls) {
                throw new IllegalArgumentException(
                        "column name '" + columnName + "' has type " + BigDecimal.class.getSimpleName() + "" +
                                " but no property '" + property + "' defined.");
            }
            return INVALID_PRECISION_OR_SCALE;
        }
        final int parsedResult;
        try {
            parsedResult = Integer.parseInt(propertyValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Couldn't parse as int value '" + propertyValue + "' for property " + property);
        }
        if (parsedResult < 1) {
            throw new IllegalArgumentException("Invalid value '" + parsedResult + "' for property " + property);
        }
        return parsedResult;
    }

    /**
     * Get a {@code PrecisionAndScale} value from a {@Properties} object.
     *
     * @param propertyNames The property names to read.
     * @param columnProperties The {@Properties} object from where to read the properties
     * @param allowNulls If true, do not throw when a property is missing, instead set the value to
     *        {@Code INVALID_PRECISION_OR_SCALE}
     * @return A {@PrecisionAndScale} object with the values read.
     */
    public static PrecisionAndScale getPrecisionAndScaleFromColumnProperties(
            final PropertyNames propertyNames,
            final Properties columnProperties,
            final boolean allowNulls) {
        final int precision = getPrecisionAndScaleFromColumnProperties(
                propertyNames.columnName,
                propertyNames.precisionProperty,
                columnProperties,
                allowNulls);
        final int scale = getPrecisionAndScaleFromColumnProperties(
                propertyNames.columnName,
                propertyNames.scaleProperty,
                columnProperties,
                allowNulls);
        return new PrecisionAndScale(precision, scale);
    }

    /**
     * Set the given names and values in the supplied {@code Properties} object.
     *
     * @param props Properties where the given property names and values would be set.
     * @param names Property names to set
     * @param values Property values to set
     */
    public static void setProperties(final Properties props, final PropertyNames names,
            final PrecisionAndScale values) {
        props.setProperty(names.precisionProperty, Integer.toString(values.precision));
        props.setProperty(names.scaleProperty, Integer.toString(values.scale));
    }
}
