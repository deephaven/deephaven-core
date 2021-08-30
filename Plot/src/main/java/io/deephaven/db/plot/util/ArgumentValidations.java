/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.errors.*;
import io.deephaven.db.plot.filters.SelectableDataSet;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.ClassUtils;

import java.util.*;

/**
 * Utilities for validating that plotting arguments are acceptable.
 */
public class ArgumentValidations {
    private static final boolean ENABLE_X_VALUE_ASSERTIONS =
        Configuration.getInstance().getBooleanWithDefault("plotting.enableXValueAssertions", true);

    /**
     * Requires the input object {@code o} to be non-null.
     *
     * @throws IllegalArgumentException {@code o} is null
     * @param d value
     * @param name variable name
     * @param plotInfo source of the exception
     */
    public static void assertGreaterThan0(double d, final String name, final PlotInfo plotInfo) {
        if (!(d > 0)) {
            throw new PlotIllegalArgumentException(name + " must be > 0", plotInfo);
        }
    }

    /**
     * Requires the input object {@code o} to be non-null.
     *
     * @throws IllegalArgumentException {@code o} is null
     * @param o object
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertNotNull(Object o, final String message, final PlotInfo plotInfo) {
        if (o == null) {
            throw new PlotIllegalArgumentException("Null input: " + message, plotInfo);
        }
    }

    /**
     * Requires the input object {@code o} to be non-null.
     *
     * @throws IllegalArgumentException {@code o} is null
     * @param o object
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static <T> void assertNotNullAndNotEmpty(T[] o, final String message,
        final PlotInfo plotInfo) {
        if (o == null || o.length == 0) {
            throw new PlotIllegalArgumentException("Null or empty input: " + message, plotInfo);
        }
    }

    public static void assertNull(Object o, final String message, final PlotInfo plotInfo) {
        if (o != null) {
            throw new PlotIllegalArgumentException("Null input: " + message, plotInfo);
        }
    }

    /**
     * Requires {@link Class} {@code c1} be an instance of {@link Class} {@code c2}.
     *
     * @throws NullPointerException {@code c1} and {@code c2} must not be null
     * @throws PlotRuntimeException {@code c1} is not assignable to {@code c2}
     * @throws IllegalArgumentException {@code o} is null
     * @param c1 class
     * @param c2 class
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertInstance(final Class c1, final Class c2, final String message,
        final PlotInfo plotInfo) {
        assertNotNull(c1, "c1", plotInfo);
        assertNotNull(c2, "c2", plotInfo);
        if (!c1.isAssignableFrom(c2)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the {@code column} of be an instance of {@link Class} {@code c}.
     *
     * @throws NullPointerException {@code t} and {@code c} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have data type {@code c}
     * @param t table
     * @param column column
     * @param c class
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertInstance(final Table t, final String column, final Class c,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final DataColumn col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        assertInstance(c, ClassUtils.primitiveToWrapper(col.getType()), message, plotInfo);
    }

    /**
     * Requires the {@code column} of be an instance of {@link Class} {@code c}.
     *
     * @throws NullPointerException {@code t} and {@code c} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have data type {@code c}
     * @param t table
     * @param column column
     * @param c class
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertInstance(final TableDefinition t, final String column, final Class c,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final ColumnDefinition col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        assertInstance(c, ClassUtils.primitiveToWrapper(col.getDataType()), message, plotInfo);
    }

    /**
     * Requires the {@code column} of be an instance of {@link Class} {@code c}.
     *
     * @throws NullPointerException {@code sds} and {@code c} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have data type {@code c}
     * @param sds swappable data set
     * @param column column
     * @param c class
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertInstance(final SelectableDataSet sds, final String column,
        final Class c, final String message, final PlotInfo plotInfo) {
        assertNotNull(sds, "sds", plotInfo);
        final ColumnDefinition col = sds.getTableDefinition().getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        assertInstance(c, ClassUtils.primitiveToWrapper(col.getDataType()), message, plotInfo);
    }


    /**
     * Requires the {@code column} of be numeric, or an instance of time, char/{@link Character}, or
     * {@link Comparable}.
     *
     * @throws NullPointerException {@code t} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have the correct data type
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTimeOrCharOrComparableInstance(final Table t,
        final String column, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumericOrTimeOrCharOrComparableInstance(t, column,
            createWrongColumnTypeErrorMessage(t, column, plotInfo), plotInfo);
    }


    /**
     * Requires the {@code column} of be numeric, or an instance of time, char/{@link Character}, or
     * {@link Comparable}.
     *
     * @throws NullPointerException {@code t} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have the correct data type
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTimeOrCharOrComparableInstance(final TableDefinition t,
        final String column, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumericOrTimeOrCharOrComparableInstance(t, column,
            createWrongColumnTypeErrorMessage(t, column, plotInfo), plotInfo);
    }

    /**
     * Requires the {@code column} of be numeric, or an instance of time, char/{@link Character}, or
     * {@link Comparable}.
     *
     * @throws NullPointerException {@code t} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have the correct data type
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTimeOrCharOrComparableInstance(final Table t,
        final String column, final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final DataColumn col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        if (!isNumericOrTime(col.getType(), plotInfo)
            && !Comparable.class.isAssignableFrom(col.getType())
            && !TypeUtils.isCharacter(col.getType())) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the {@code column} of be numeric, or an instance of time, char/{@link Character}, or
     * {@link Comparable}.
     *
     * @throws NullPointerException {@code t} must not be null
     * @throws PlotRuntimeException {@code column} is not a column of {@code t}, or {@code column}
     *         does not have the correct data type
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTimeOrCharOrComparableInstance(final TableDefinition t,
        final String column, final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final ColumnDefinition col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        if (!isNumericOrTime(col.getDataType(), plotInfo)
            && !Comparable.class.isAssignableFrom(col.getDataType())
            && !TypeUtils.isCharacter(col.getDataType())) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires {@code data} and {@code dataNames} 1. contain the same number of members. 2. contain
     * no null members
     *
     * All members of {@code data} must be the same size.
     *
     * @throws PlotIllegalStateException {@code data} and {@code dataNames} are not the same size
     * @throws PlotIllegalArgumentException a member of {@code data} or {@code dataNames} is null 2+
     *         members of {@code data} are not the same size
     *
     * @param data array of {@link IndexableNumericData}
     * @param dataNames names for each {@link IndexableNumericData}
     * @param plotInfo source of the exception
     */
    public static void assertSameSize(final IndexableNumericData[] data, final String[] dataNames,
        final PlotInfo plotInfo) {
        assertNotNull(data, "data", plotInfo);
        assertNotNull(dataNames, "dataNames", plotInfo);
        if (data.length != dataNames.length) {
            throw new PlotIllegalStateException("Inputs must be of the same dimension", plotInfo);
        }

        for (int i = 0; i < data.length; i++) {
            assertNotNull(data[i], dataNames[i], plotInfo);
        }

        for (int i = 0; i < data.length; i++) {
            if (data[i].size() != data[0].size()) {
                throw new PlotIllegalArgumentException("Input data is of inconsistent size: ("
                    + dataNames[i] + "=" + data[i] + "," + dataNames[0] + "=" + data[0] + ")",
                    plotInfo);
            }
        }
    }

    /**
     * Gets the data type of the {@code column}.
     *
     * @throws PlotRuntimeException {@code column} not in the table
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return data type of {@code column}
     */
    public static Class getColumnType(final Table t, final String column, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final DataColumn col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        return col.getType();
    }

    /**
     * Gets the data type of the {@code column}.
     *
     * @throws PlotRuntimeException {@code column} not in the table
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return data type of {@code column}
     */
    public static Class getColumnType(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        final ColumnDefinition col = t.getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        return col.getDataType();
    }

    /**
     * Gets the data type of the {@code column}.
     *
     * @throws PlotRuntimeException {@code column} not in the table
     * @param sds selectable dataset
     * @param column column
     * @param plotInfo source of the exception
     * @return data type of {@code column}
     */
    public static Class getColumnType(final SelectableDataSet sds, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(sds, "sds", plotInfo);
        final ColumnDefinition col = sds.getTableDefinition().getColumn(column);

        if (col == null) {
            throw new PlotRuntimeException("Column not present in table: column=" + column,
                plotInfo);
        }

        return col.getDataType();
    }

    /**
     * Whether the class is equal to Date.class or DBDateTime.class
     *
     * @param c class
     * @param plotInfo source of the exception
     * @return true if {@code c} equals Date.class or DBDateTime.class, false otherwise
     */
    public static boolean isTime(final Class c, final PlotInfo plotInfo) {
        assertNotNull(c, "c", plotInfo);
        return c.equals(Date.class) || c.equals(DBDateTime.class);
    }

    /**
     * Whether the class is {@link TypeUtils#isNumeric(Class)} or {@link #isTime(Class, PlotInfo)}
     *
     * @param c class
     * @return true if {@code c} is a numeric or time class, false otherwise
     */
    public static boolean isNumericOrTime(final Class c) {
        return TypeUtils.isNumeric(c) || isTime(c, null);
    }

    /**
     * Whether the class is {@link TypeUtils#isNumeric(Class)} or {@link #isTime(Class, PlotInfo)}
     *
     * @param c class
     * @param plotInfo source of the exception
     * @return true if {@code c} is a numeric or time class, false otherwise
     */
    public static boolean isNumericOrTime(final Class c, final PlotInfo plotInfo) {
        return TypeUtils.isNumeric(c) || isTime(c, plotInfo);
    }

    /**
     * Whether the column's data type equals Date.class or DBDateTime.class
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type equals Date.class or DBDateTime.class, false otherwise
     */
    public static boolean isTime(final Table t, final String column, final PlotInfo plotInfo) {
        return isTime(getColumnType(t, column, plotInfo), plotInfo);
    }

    /**
     * Whether the column's data type equals Date.class or DBDateTime.class
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type equals Date.class or DBDateTime.class, false otherwise
     */
    public static boolean isTime(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        return isTime(getColumnType(t, column, plotInfo), plotInfo);
    }

    /**
     * Whether the column's data type equals Date.class or DBDateTime.class
     *
     * @param sds selectable dataset
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type equals Date.class or DBDateTime.class, false otherwise
     */
    public static boolean isTime(final SelectableDataSet sds, final String column,
        final PlotInfo plotInfo) {
        return isTime(getColumnType(sds, column, plotInfo), plotInfo);
    }

    /**
     * Whether the column's data type {@link TypeUtils#isPrimitiveNumeric(Class)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is a numeric primitive, false otherwise
     */
    public static boolean isPrimitiveNumeric(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return TypeUtils.isPrimitiveNumeric(getColumnType(t, column, plotInfo));
    }

    /**
     * Whether the column's data type {@link TypeUtils#isBoxedNumeric(Class)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is a boxed numeric, false otherwise
     */
    public static boolean isBoxedNumeric(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return TypeUtils.isBoxedNumeric(getColumnType(t, column, plotInfo));
    }

    /**
     * Whether the column's data type {@link TypeUtils#isNumeric(Class)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is numeric, false otherwise
     */
    public static boolean isNumeric(final Table t, final String column, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return TypeUtils.isNumeric(getColumnType(t, column, plotInfo));
    }

    /**
     * Whether the column's data type {@link TypeUtils#isNumeric(Class)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is numeric, false otherwise
     */
    public static boolean isNumeric(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return TypeUtils.isNumeric(getColumnType(t, column, plotInfo));
    }

    /**
     * Whether the column's data type {@link TypeUtils#isNumeric(Class)}.
     *
     * @param sds selectable dataset
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is numeric, false otherwise
     */
    public static boolean isNumeric(final SelectableDataSet sds, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(sds, "t", plotInfo);
        return TypeUtils.isNumeric(getColumnType(sds, column, plotInfo));
    }

    /**
     * Whether the column's data type {@link #isNumericOrTime(Class, PlotInfo)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is a numeric or time instance, false otherwise
     */
    public static boolean isNumericOrTime(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return isNumericOrTime(getColumnType(t, column, plotInfo), plotInfo);
    }

    /**
     * Whether the column's data type {@link #isNumericOrTime(Class, PlotInfo)}.
     *
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is a numeric or time instance, false otherwise
     */
    public static boolean isNumericOrTime(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        return isNumericOrTime(getColumnType(t, column, plotInfo), plotInfo);
    }

    /**
     * Whether the column's data type {@link #isNumericOrTime(Class, PlotInfo)}.
     *
     * @param sds selectable dataset
     * @param column column
     * @param plotInfo source of the exception
     * @return true if the column's data type is a numeric or time instance, false otherwise
     */
    public static boolean isNumericOrTime(final SelectableDataSet sds, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(sds, "sds", plotInfo);
        return isNumericOrTime(getColumnType(sds, column, plotInfo), plotInfo);
    }

    /**
     * Requires the column's data type to be a time instance as defined in
     * {@link #isTime(Class, PlotInfo)}
     *
     * @throws RuntimeException if the column's data type isn't a time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsTime(final Table t, final String column, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsTime(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo), plotInfo);
    }

    /**
     * Requires the column's data type to be a time instance as defined in
     * {@link #isTime(Class, PlotInfo)}
     *
     * @throws RuntimeException if the column's data type isn't a time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsTime(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsTime(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo), plotInfo);
    }

    /**
     * Requires the column's data type to be a time instance as defined in
     * {@link #isTime(Class, PlotInfo)}
     *
     * @throws RuntimeException if the column's data type isn't a time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @param message error message
     */
    public static void assertIsTime(final Table t, final String column, final String message,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isTime(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be a time instance as defined in
     * {@link #isTime(Class, PlotInfo)}
     *
     * @throws RuntimeException if the column's data type isn't a time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @param message error message
     */
    public static void assertIsTime(final TableDefinition t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isTime(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be a numeric primitive as defined in
     * {@link TypeUtils#isPrimitiveNumeric(Class)}
     *
     * @throws RuntimeException if the column's data type isn't a numeric primitive
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsPrimitiveNumeric(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsPrimitiveNumeric(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo),
            plotInfo);
    }

    /**
     * Requires the column's data type to be a numeric primitive as defined in
     * {@link TypeUtils#isPrimitiveNumeric(Class)}
     *
     * @throws RuntimeException if the column's data type isn't a numeric primitive
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     * @param message error message
     */
    public static void assertIsPrimitiveNumeric(final Table t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isPrimitiveNumeric(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be an instance of {@link Number} as defined in
     * {@link TypeUtils#isBoxedNumeric(Class)}
     *
     * @throws RuntimeException if the column's data type isn't an instance of {@link Number}
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsBoxedNumeric(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsBoxedNumeric(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo),
            plotInfo);
    }

    /**
     * Requires the column's data type to be an instance of {@link Number} as defined in
     * {@link TypeUtils#isBoxedNumeric(Class)}
     *
     * @throws RuntimeException if the column's data type isn't an instance of {@link Number}
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsBoxedNumeric(final Table t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isBoxedNumeric(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }


    /**
     * Requires the column's data type to be a numeric instance as defined in
     * {@link TypeUtils#isNumeric(Class)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumeric(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumeric(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo),
            plotInfo);
    }


    /**
     * Requires the column's data type to be a numeric instance as defined in
     * {@link TypeUtils#isNumeric(Class)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumeric(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumeric(t, column, createWrongColumnTypeErrorMessage(t, column, plotInfo),
            plotInfo);
    }


    /**
     * Requires the column's data type to be a numeric instance as defined in
     * {@link TypeUtils#isNumeric(Class)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric instance
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumeric(final Table t, final String column, final String message,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isNumeric(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }


    /**
     * Requires the column's data type to be a numeric instance as defined in
     * {@link TypeUtils#isNumeric(Class)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric instance
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumeric(final TableDefinition t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isNumeric(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }


    /**
     * Requires the column's data type to be a numeric instance as defined in
     * {@link TypeUtils#isNumeric(Class)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric instance
     * @param sds selectable dataset
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumeric(final SelectableDataSet sds, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(sds, "t", plotInfo);
        if (!isNumeric(sds, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final Table t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumericOrTime(t, column,
            createWrongColumnTypeErrorMessage(t, column, plotInfo, "Numeric, Time"), plotInfo);
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param t table
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final TableDefinition t, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        assertIsNumericOrTime(t, column,
            createWrongColumnTypeErrorMessage(t, column, plotInfo, "Numeric, Time"), plotInfo);
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param sds selectable dataset
     * @param column column
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final SelectableDataSet sds, final String column,
        final PlotInfo plotInfo) {
        assertNotNull(sds, "sds", plotInfo);
        assertIsNumericOrTime(sds, column,
            createWrongColumnTypeErrorMessage(sds, column, plotInfo, "Numeric, Time"), plotInfo);
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final Table t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isNumericOrTime(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param t table
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final TableDefinition t, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(t, "t", plotInfo);
        if (!isNumericOrTime(t, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires the column's data type to be a numeric or time instance as defined in
     * {@link #isNumericOrTime(Class, PlotInfo)}
     *
     * @throws PlotRuntimeException if the column's data type isn't a numeric or time instance
     * @param sds selectable dataset
     * @param column column
     * @param message error message
     * @param plotInfo source of the exception
     */
    public static void assertIsNumericOrTime(final SelectableDataSet sds, final String column,
        final String message, final PlotInfo plotInfo) {
        assertNotNull(sds, "t", plotInfo);
        if (!isNumericOrTime(sds, column, plotInfo)) {
            throw new PlotRuntimeException(message, plotInfo);
        }
    }

    /**
     * Requires all columns in {@code cols} be columns in the table.
     *
     * @throws IllegalArgumentException if the table does not contain all columns
     * @param t table
     * @param plotInfo source of the exception
     * @param cols column names
     */
    public static void assertColumnsInTable(final Table t, final PlotInfo plotInfo,
        final String... cols) {
        assertNotNull(t, "t", plotInfo);
        assertNotNull(cols, "cols", plotInfo);
        for (String c : cols) {
            if (!t.getColumnSourceMap().containsKey(c)) {
                throw new PlotIllegalArgumentException(
                    "Column " + c + " could not be found in table.", plotInfo);
            }
        }
    }

    /**
     * Requires all columns in {@code cols} be columns in the table.
     *
     * @throws IllegalArgumentException if the table does not contain all columns
     * @param t table
     * @param plotInfo source of the exception
     * @param cols column names
     */
    public static void assertColumnsInTable(final TableDefinition t, final PlotInfo plotInfo,
        final String... cols) {
        assertNotNull(t, "t", plotInfo);
        assertNotNull(cols, "cols", plotInfo);
        for (String c : cols) {
            if (t.getColumn(c) == null) {
                throw new PlotIllegalArgumentException(
                    "Column " + c + " could not be found in table.", plotInfo);
            }
        }
    }

    /**
     * Requires all columns in {@code cols} be columns in the selectable dataset.
     *
     * @throws IllegalArgumentException if the table does not contain all columns
     * @param sds selectable dataset
     * @param plotInfo source of the exception
     * @param cols column names
     */
    public static void assertColumnsInTable(final SelectableDataSet sds, final PlotInfo plotInfo,
        final String... cols) {
        assertNotNull(sds, "t", plotInfo);
        assertNotNull(cols, "cols", plotInfo);

        final Set<String> colnames = new HashSet<>(sds.getTableDefinition().getColumnNames());
        for (String c : cols) {
            if (!colnames.contains(c)) {
                throw new PlotIllegalArgumentException(
                    "Column " + c + " could not be found in selectable dataset.", plotInfo);
            }
        }
    }

    /**
     * Requires all columns in {@code cols} be columns in the table held by the handle.
     *
     * @throws IllegalArgumentException if the underlying table does not contain all columns
     * @param t table handle
     * @param plotInfo source of the exception
     * @param cols column names
     */
    public static void assertColumnsInTable(final TableHandle t, final PlotInfo plotInfo,
        final String... cols) {
        assertNotNull(t, "t", plotInfo);
        assertNotNull(cols, "cols", plotInfo);
        for (String c : cols) {
            if (!t.hasColumns(c)) {
                throw new PlotIllegalArgumentException(
                    "Column " + c + " could not be found in table.", plotInfo);
            }
        }
    }

    public static boolean nanSafeEquals(double x, double x1) {
        return x == x1 || (Double.isNaN(x) && Double.isNaN(x1));
    }

    private static String createWrongColumnTypeErrorMessage(final Table t, final String column,
        final PlotInfo plotInfo, final String... types) {
        assertNotNull(t, "t", plotInfo);
        return "Invalid data type in column = " + column + ". Expected one of "
            + Arrays.toString(types) + ", was " + getColumnType(t, column, plotInfo);
    }

    private static String createWrongColumnTypeErrorMessage(final TableDefinition t,
        final String column, final PlotInfo plotInfo, final String... types) {
        assertNotNull(t, "t", plotInfo);
        return "Invalid data type in column = " + column + ". Expected one of "
            + Arrays.toString(types) + ", was " + getColumnType(t, column, plotInfo);
    }

    private static String createWrongColumnTypeErrorMessage(final SelectableDataSet sds,
        final String column, final PlotInfo plotInfo, final String... types) {
        assertNotNull(sds, "sds", plotInfo);
        return "Invalid data type in column = " + column + ". Expected one of "
            + Arrays.toString(types) + ", was " + getColumnType(sds, column, plotInfo);
    }
}
