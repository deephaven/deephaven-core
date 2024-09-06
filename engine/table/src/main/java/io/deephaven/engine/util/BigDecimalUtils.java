//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ObjectVectorColumnWrapper;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Properties;

/**
 * Utilities to support BigDecimal exhaust.
 * <p>
 * Parquet and Avro decimal types make a whole column decimal type have a fixed precision and scale; BigDecimal columns
 * in Deephaven are, each value, arbitrary precision (its own precision and scale).
 * <p>
 * For static tables, it is possible to compute overall precision and scale values that fit every existing value. For
 * refreshing tables, we need the user to tell us.
 */
public class BigDecimalUtils {

    public static final int INVALID_PRECISION_OR_SCALE = -1;

    private static final PrecisionAndScale EMPTY_TABLE_PRECISION_AND_SCALE = new PrecisionAndScale(1, 1);
    private static final int INIT_MAX_PRECISION_MINUS_SCALE = -1;
    private static final int INIT_MAX_SCALE = -1;

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
     * @param table A Deephaven table
     * @param colName Column for {@code table}, which should be of {@code BigDecimal} {@link ColumnSource#getType type}
     *        or {@link ColumnSource#getComponentType component type}
     * @return A {@link PrecisionAndScale} object result.
     */
    public static PrecisionAndScale computePrecisionAndScale(
            final Table table,
            final String colName) {
        final ColumnSource<?> src = table.getColumnSource(colName);
        return computePrecisionAndScale(table.getRowSet(), src);
    }

    /**
     * Compute an overall precision and scale that would fit all existing values in a column source. Note that this
     * requires a full table scan to ensure the correct values are determined.
     *
     * @param rowSet The rowset for the provided column
     * @param columnSource A {@code ColumnSource} of {@code BigDecimal} {@link ColumnSource#getType type} or
     *        {@link ColumnSource#getComponentType component type}
     * @return A {@link PrecisionAndScale} object result.
     */
    public static PrecisionAndScale computePrecisionAndScale(
            final RowSet rowSet,
            final ColumnSource<?> columnSource) {
        if (rowSet.isEmpty()) {
            return EMPTY_TABLE_PRECISION_AND_SCALE;
        }

        // We will walk the entire table to determine the max(precision - scale) and
        // max(scale), which corresponds to max(digits left of the decimal point), max(digits right of the decimal
        // point). Then we convert to (precision, scale) before returning.
        final BigDecimalParameters result = new BigDecimalParameters(INIT_MAX_PRECISION_MINUS_SCALE, INIT_MAX_SCALE);
        final ObjectVector<?> columnVector = new ObjectVectorColumnWrapper<>(columnSource, rowSet);
        try (final CloseableIterator<?> columnIterator = columnVector.iterator()) {
            final Class<?> columnType = columnSource.getType();
            if (columnType == BigDecimal.class) {
                // noinspection unchecked
                processFlatColumn((Iterator<BigDecimal>) columnIterator, result);
            } else if (columnSource.getComponentType() == BigDecimal.class) {
                if (columnType.isArray()) {
                    // noinspection unchecked
                    processArrayColumn((Iterator<BigDecimal[]>) columnIterator, result);
                } else if (Vector.class.isAssignableFrom(columnType)) {
                    // noinspection unchecked
                    processVectorColumn((Iterator<ObjectVector<BigDecimal>>) columnIterator, result);
                }
            } else {
                throw new IllegalArgumentException("Column source is not of type BigDecimal or an array/vector of " +
                        "BigDecimal, but of type " + columnType + " and component type " +
                        columnSource.getComponentType());
            }
        }

        // If these are same as initial values, then every value we visited was null
        if (result.maxPrecisionMinusScale == INIT_MAX_PRECISION_MINUS_SCALE && result.maxScale == INIT_MAX_SCALE) {
            return EMPTY_TABLE_PRECISION_AND_SCALE;
        }

        return new PrecisionAndScale(result.maxPrecisionMinusScale + result.maxScale, result.maxScale);
    }

    private static class BigDecimalParameters {
        private int maxPrecisionMinusScale;
        private int maxScale;

        private BigDecimalParameters(final int maxPrecisionMinusScale, final int maxScale) {
            this.maxPrecisionMinusScale = maxPrecisionMinusScale;
            this.maxScale = maxScale;
        }

        /**
         * Update the maximum values for the parameters based on the given value.
         */
        private void updateMaximum(@Nullable final BigDecimal value) {
            if (value == null) {
                return;
            }
            final int precision = value.precision();
            final int scale = value.scale();
            final int precisionMinusScale = precision - scale;
            if (precisionMinusScale > maxPrecisionMinusScale) {
                maxPrecisionMinusScale = precisionMinusScale;
            }
            if (scale > maxScale) {
                maxScale = scale;
            }
        }
    }

    private static void processFlatColumn(
            @NotNull final Iterator<BigDecimal> columnIterator,
            @NotNull final BigDecimalParameters result) {
        columnIterator.forEachRemaining(result::updateMaximum);
    }

    private static void processVectorColumn(
            @NotNull final Iterator<ObjectVector<BigDecimal>> columnIterator,
            @NotNull final BigDecimalParameters result) {
        columnIterator.forEachRemaining(values -> {
            if (values == null) {
                return;
            }
            try (final CloseableIterator<BigDecimal> valuesIterator = values.iterator()) {
                valuesIterator.forEachRemaining(result::updateMaximum);
            }
        });
    }

    private static void processArrayColumn(
            @NotNull final Iterator<BigDecimal[]> columnIterator,
            @NotNull final BigDecimalParameters result) {
        columnIterator.forEachRemaining(values -> {
            if (values == null) {
                return;
            }
            for (final BigDecimal value : values) {
                result.updateMaximum(value);
            }
        });
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
