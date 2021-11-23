/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.engine.util.TableTools;
import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import java.io.IOException;
import java.io.Serializable;

/**
 * Utilities for cumulative and rolling aggregations.
 */
public class CumulativeUtil {

    /**
     * Provides a helper class with a map to cache previous row values when performing rolling or cumulative
     * aggregations.
     */
    @SuppressWarnings("unchecked")
    public static class CumulativeHelper implements Serializable {
        private TObjectDoubleHashMap map;

        public CumulativeHelper(double startValue) {
            map = new TObjectDoubleHashMap(100, Constants.DEFAULT_LOAD_FACTOR, startValue);
        }

        public double get(Object key) {
            return map.get(key);
        }

        public double put(Object key, double val) {
            map.put(key, val);
            return val;
        }
    }

    /**
     * Provides a helper class with a map to cache previous row values when performing filtering of data whose values
     * change from row to row.
     */
    @SuppressWarnings("unchecked")
    public static class CumulativeFilterHelper implements Serializable {
        private TObjectDoubleHashMap map;

        public CumulativeFilterHelper() {
            map = new TObjectDoubleHashMap(100);
        }

        public boolean shouldKeep(Object key, double val) {
            if (!map.containsKey(key)) {
                map.put(key, val);
                return true;
            }

            return val != map.put(key, val);
        }
    }

    /**
     * General purpose core method for executing running and cumulative aggregations. Used by helper methods which
     * provide template formulae for specific types of aggregations.
     * 
     * @param t The {@link Table} to use as input to the aggregation.
     * @param key Nullable string key to use to access the cached previous row value within the {@link CumulativeHelper}
     *        map. Note that a non-null literal key must be enclosed in backticks since it will be used in a formula.
     *        Typically a column expression, rather than a literal, is used to allow operations based on the grouping of
     *        the expression.
     * @param startValue Initial value from which to start aggregating. Normally 0.
     * @param newCol The name of the aggregation column to add to the table.
     * @param formula A formula indicating how to calculate the aggregation. This is normally provides by a helper
     *        method. An example would be something like: "_prev+A" to cumulatively sum A.
     * @return A {@link Table} with the new aggregation column added. Note that this column will be a double column,
     *         regardless of the numeric type(s) of the formula input(s).
     */
    public static Table accumulate(Table t, String key, double startValue, String newCol, String formula) {
        QueryScope.addParam("__CumulativeUtil_map", new CumulativeHelper(startValue));

        formula = newCol + "=__CumulativeUtil_map.put(" + key + ","
                + formula.replaceAll("_prev", "__CumulativeUtil_map.get(" + key + ")") + ")";

        return t.update(formula);
    }

    /**
     * Executes a cumulative aggregation of the minimum value so far for a formula.
     * 
     * @param t The {@link Table} to use as input to the aggregation.
     * @param newCol The name of the aggregation column to add to the table.
     * @param formula A formula for the source value on which to calculate a running minimum. This can be as simple as
     *        the column name or a more complex expression.
     * @return A {@link Table} with the new aggregation column added. Note that this column will be a double column,
     *         regardless of the numeric type(s) of the formula input(s).
     */
    public static Table cumMin(Table t, String newCol, String formula) {
        return accumulate(t, null, Double.MAX_VALUE, newCol, "min(_prev, (double)" + formula + ")");
    }

    /**
     * Executes a cumulative sum aggregation far for a formula.
     * 
     * @param t The {@link Table} to use as input to the aggregation.
     * @param key Nullable string key to use to access the cached previous row value within the {@link CumulativeHelper}
     *        map. Note that a non-null literal key must be enclosed in backticks since it will be used in a formula.
     *        Typically a column expression, rather than a literal, is used to allow operations based on the grouping of
     *        the expression.
     * @param newCol The name of the aggregation column to add to the table.
     * @param formula A formula for the source value on which to calculate a running sum. This can be as simple as the
     *        column name or a more complex expression.
     * @return A {@link Table} with the new aggregation column added. Note that this column will be a double column,
     *         regardless of the numeric type(s) of the formula input(s).
     */
    public static Table cumSum(Table t, String key, String newCol, String formula) {
        return accumulate(t, key, 0, newCol, "_prev+(" + formula + ")");
    }

    /**
     * Executes a cumulative sum aggregation far for a formula.
     * 
     * @param t The {@link Table} to use as input to the aggregation.
     * @param newCol The name of the aggregation column to add to the table.
     * @param formula A formula for the source value on which to calculate a running sum. This can be as simple as the
     *        column name or a more complex expression.
     * @return A {@link Table} with the new aggregation column added. Note that this column will be a double column,
     *         regardless of the numeric type(s) of the formula input(s).
     */
    public static Table cumSum(Table t, String newCol, String formula) {
        return accumulate(t, null, 0, newCol, "_prev+(" + formula + ")");
    }

    /**
     * Executes a rolling sum aggregation far for a formula.
     * 
     * @param t The {@link Table} to use as input to the aggregation.
     * @param windowSize The number of rows to include in the rolling sum window.
     * @param newCol The name of the aggregation column to add to the table.
     * @param formula A formula for the source value on which to calculate a running sum. This can be as simple as the
     *        column name or a more complex expression.
     * @return A {@link Table} with the new aggregation column added. Note that this column will be a double column,
     *         regardless of the numeric type(s) of the formula input(s).
     */
    public static Table rollingSum(Table t, int windowSize, String newCol, String formula) {
        return accumulate(t, null, 0, newCol, "_prev+(" + formula + ")")
                .update(newCol + "=" + newCol + "-" + newCol + "_[i-" + windowSize + "]");
    }

    /**
     * Returns only rows for which the selected column value is different from the value in the previous row.
     * 
     * @param t The {@link Table} to use as input to the method.
     * @param key Nullable string key to use to access the cached previous row value within the {@link CumulativeHelper}
     *        map. Note that a non-null literal key must be enclosed in backticks since it will be used in a formula.
     *        Typically a column expression, rather than a literal, is used to allow operations based on the grouping of
     *        the expression.
     * @param col The column to check for changing values.
     * @return A {@link Table} of only rows where the selected value has changed from the value in the previous row.
     */
    public static Table filterChanged(Table t, String key, String col) {
        QueryScope.addParam("__CumulativeUtil_map", new CumulativeFilterHelper());

        return t.where("__CumulativeUtil_map.shouldKeep(" + key + ", " + col + ")");
    }

    /**
     * main method to show examples of use of this class' methods.
     * 
     * @param args Not used
     * @throws IOException
     */
    public static void main(String... args) throws IOException {
        Table t = TableTools.emptyTable(20).update("USym=Math.random()>.5 ? `SPY` : `AAPL`", "Num=Math.random()");

        Table test = accumulate(t, "USym", Double.MAX_VALUE, "Min", "min(_prev, Num)");
        TableTools.show(test);
        TableTools.show(filterChanged(test, "USym", "Min"));

        System.out.println();
        System.out.println();
        System.out.println();

        TableTools.show(accumulate(t, null, Double.MAX_VALUE, "Min", "min(_prev, Num)"));
        TableTools.show(cumMin(t, "Min", "Num"));

        System.out.println();
        System.out.println();
        System.out.println();

        TableTools.show(cumSum(t, "Sum", "Num*2"));

        TableTools.show(rollingSum(t, 2, "Sum", "Num"));
    }
}
