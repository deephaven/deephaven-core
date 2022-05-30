/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm.util;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.vector.*;
import io.deephaven.time.DateTime;

/**
 * Utilities for building model farms.
 */
public class ModelFarmUtils {
    private ModelFarmUtils() {}

    /**
     * Require a table to have specified columns of specified types.
     *
     * @param tableName table name
     * @param t table
     * @param colNames required column names
     * @param colTypes required column types
     */
    public static void requireTable(final String tableName, final Table t, final String[] colNames,
            final Class[] colTypes) {
        Require.eq(colNames.length, "colNames.length", colTypes.length, "colTypes.length");

        for (int i = 0; i < colNames.length; i++) {
            final String cn = colNames[i];
            final Class ct = colTypes[i];
            Require.eqTrue(t.hasColumns(cn), "Table is missing column.  tableName=" + tableName + " columnName=" + cn);
            final Class cta = t.getDefinition().getColumn(cn).getDataType();
            Require.eqTrue(cta.equals(ct), "Table column is of the wrong type.  tableName=" + tableName + " columnName="
                    + cn + " typeRequired=" + ct + " typeActual=" + cta);
        }
    }

    /**
     * Interpret a table cell value as a string array.
     * 
     * @param o table cell value.
     * @return string array.
     */
    public static String[] arrayString(final Object o) {
        // noinspection unchecked
        return o == null ? null : ((ObjectVector<String>) o).toArray();
    }

    /**
     * Interpret a table cell value as a date time array.
     * 
     * @param o table cell value.
     * @return date time array.
     */
    public static DateTime[] arrayDateTime(final Object o) {
        // noinspection unchecked
        return o == null ? null : ((ObjectVector<DateTime>) o).toArray();
    }

    /**
     * Interpret a table cell value as a float array.
     * 
     * @param o table cell value.
     * @return float array.
     */
    public static float[] arrayFloat(final Object o) {
        return o == null ? null : ((FloatVector) o).toArray();
    }

    /**
     * Interpret a table cell value as a double array.
     * 
     * @param o table cell value.
     * @return double array.
     */
    public static double[] arrayDouble(final Object o) {
        return o == null ? null : ((DoubleVector) o).toArray();
    }

    /**
     * Interpret a table cell value as an int array.
     * 
     * @param o table cell value.
     * @return int array.
     */
    public static int[] arrayInt(final Object o) {
        return o == null ? null : ((IntVector) o).toArray();
    }

    /**
     * Interpret a table cell value as a long array.
     * 
     * @param o table cell value.
     * @return long array.
     */
    public static long[] arrayLong(final Object o) {
        return o == null ? null : ((LongVector) o).toArray();
    }

    /**
     * Interpret a table cell value as a 2D double array.
     * 
     * @param o table cell value.
     * @return 2D double array.
     */
    public static double[][] array2Double(final Object o) {
        if (o == null) {
            return null;
        }

        final ObjectVector<DoubleVector> a = (ObjectVector<DoubleVector>) o;
        final double[][] result = new double[a.intSize()][];

        for (int i = 0; i < a.intSize(); i++) {
            result[i] = a.get(i).toArray();
        }

        return result;
    }

}
