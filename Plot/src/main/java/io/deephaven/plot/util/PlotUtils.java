/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.util;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.base.verify.Require;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.plot.ChartImpl;
import io.deephaven.plot.datasets.category.CategoryDataSeries;
import io.deephaven.plot.datasets.data.*;
import io.deephaven.plot.datasets.interval.IntervalXYDataSeriesArray;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.time.DateTime;
import io.deephaven.gui.color.ColorPaletteArray;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.ColorPalette;
import io.deephaven.gui.color.Paint;
import io.deephaven.gui.table.filters.Condition;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.deephaven.api.agg.Aggregation.AggCount;
import static io.deephaven.api.agg.Aggregation.AggLast;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Numeric.abs;

/**
 * Utilities class for plotting.
 */
public class PlotUtils {

    private PlotUtils() {}

    /** Instances of ColorPaletteArray have some state, so this is kept privat. */
    private static final ColorPaletteArray MATPLOT_COLORS = new ColorPaletteArray(ColorPaletteArray.Palette.MATPLOTLIB);

    private static final Random rng = new Random();

    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final long[] EMPTY_LONG_ARRAY = new long[0];
    private static final short[] EMPTY_SHORT_ARRAY = new short[0];
    private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    private static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    private static final char[] EMPTY_CHAR_ARRAY = new char[0];
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Number[] EMPTY_NUMBER_ARRAY = new Number[0];
    private static final Date[] EMPTY_DATE_ARRAY = new Date[0];
    private static final DateTime[] EMPTY_DATETIME_ARRAY = new DateTime[0];

    private static int randVar() {
        return abs(rng.nextInt());
    }

    /**
     * Gets a variable name not already in the {@link QueryScope} by appending random integers to the end of
     * {@code root} until a unique name is found.
     *
     * @param root base variable name
     * @return unique randomized variable name based off {@code root}
     */
    public static String uniqueVarName(final String root) {
        while (true) {
            final String name = root + randVar();

            try {
                QueryScope.getParamValue(name);
            } catch (QueryScope.MissingVariableException e) {
                return name;
            }
        }
    }

    /**
     * Gets the color of the {@code chart}'s color palette at the specified index {@code color}.
     *
     * @param chart chart
     * @param color index
     * @return color of the {@code chart} at the index {@code color}
     */
    @Deprecated
    public static Paint intToColor(final ChartImpl chart, final Integer color) {
        return intToColor(color);
    }

    public static Paint intToColor(final Integer color) {
        return color == null || color == NULL_INT || color < 0 ? null : MATPLOT_COLORS.get(color);
    }

    /**
     * Gets the double equivalent of the {@link Number}. Null {@link QueryConstants} are converted to Double.NaN.
     *
     * @param n number
     * @return double value of {@code n}
     * @throws UnsupportedOperationException {@code n} isn't a supported data type
     */
    public static double numberToDouble(final Number n) {
        if (n == null) {
            return Double.NaN;
        } else if (n instanceof Short) {
            return n.shortValue() == QueryConstants.NULL_SHORT ? Double.NaN : n.doubleValue();
        } else if (n instanceof Integer) {
            return n.intValue() == QueryConstants.NULL_INT ? Double.NaN : n.doubleValue();
        } else if (n instanceof Long) {
            return n.longValue() == QueryConstants.NULL_LONG ? Double.NaN : n.doubleValue();
        } else if (n instanceof Float) {
            return n.floatValue() == QueryConstants.NULL_FLOAT ? Double.NaN : n.doubleValue();
        } else if (n instanceof Double) {
            return n.doubleValue() == QueryConstants.NULL_DOUBLE ? Double.NaN : n.doubleValue();
        } else if (n instanceof BigDecimal) {
            return n.doubleValue() == QueryConstants.NULL_DOUBLE ? Double.NaN : n.doubleValue();
        } else {
            throw new UnsupportedOperationException("Unsupported Number type: " + n.getClass());
        }
    }

    /**
     * Creates a new table with a column holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static <T> Table table(final T[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.col(colName, x));
    }

    /**
     * Creates a new table with a column holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @param <T> type of the data in {@code x}
     * @return new table with column holding {@code x}
     */
    public static <T> Table table(final List<T> x, final String colName) {
        Require.neqNull(x, "x");
        final Object[] xx = x.toArray();
        return TableTools.newTable(TableTools.col(colName, xx));
    }

    /**
     * Creates a new table with a column of shorts holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table table(final short[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.shortCol(colName, x));
    }

    /**
     * Creates a new table with a column of ints holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table table(final int[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.intCol(colName, x));
    }

    /**
     * Creates a new table with a column of longs holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table table(final long[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.longCol(colName, x));
    }

    /**
     * Creates a new table with a column of floats holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table table(final float[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.floatCol(colName, x));
    }

    /**
     * Creates a new table with a column of doubles holding the specified data.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table table(final double[] x, final String colName) {
        Require.neqNull(x, "x");
        return TableTools.newTable(TableTools.doubleCol(colName, x));
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @param <T> type of the data in {@code x}
     * @return new table with column holding {@code x}
     */
    public static <T extends Number> Table doubleTable(final T[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = X==null ? Double.NaN : X.doubleValue()");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @param <T> type of the data in {@code x}
     * @return new table with column holding {@code x}
     */
    public static <T extends Number> Table doubleTable(final List<T> x, final String colName) {
        return table(x, "X")
                .updateView("__Class__ = X.getClass()")
                .view(colName + " = __Class__==Double.class ? X=NULL_DOUBLE ? Double.NaN : ((Double) X).doubleValue() "
                        +
                        ": __Class__==Short.class ? X=NULL_SHORT ? Double.NaN : ((Short) X).doubleValue() " +
                        ": __Class__==Long.class ? X=NULL_LONG ? Double.NaN : ((Long) X).doubleValue() " +
                        ": __Class__==Integer.class ? X=NULL_INT ? Double.NaN : ((Integer) X).doubleValue() " +
                        ": __Class__==Float.class ? X==NULL_FLOAT ? Double.NaN : ((Float) X).doubleValue()" +
                        ": Double.NaN");

    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final short[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final int[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final long[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final float[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final double[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /*
     * Auto unboxing of these types will throw an error in the generic method public static <T extends Number> Table
     * doubleTable(final T[] x, final String colName)
     */

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final Double[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final Short[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final Long[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final Float[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new table with a column of doubles holding the specified data. Values of {@code x} are converted to
     * their corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param colName column name
     * @return new table with column holding {@code x}
     */
    public static Table doubleTable(final Integer[] x, final String colName) {
        return table(x, "X")
                .view(colName + " = isNull(X) ? Double.NaN : X");
    }

    /**
     * Creates a new array of floats holding the specified data. Values of {@code x} are converted to their
     * corresponding float values. Nulls are mapped to Float.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static float[] toFloat(final double[] x) {
        if (x == null) {
            return null;
        }
        final float[] result = new float[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_DOUBLE ? Float.NaN : (float) x[i]);
        return result;
    }

    /**
     * Creates a new array of floats holding the specified data. Values of {@code x} are converted to their
     * corresponding float values. Nulls are mapped to Float.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static float[] toFloat(final int[] x) {
        if (x == null) {
            return null;
        }
        final float[] result = new float[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_INT ? Float.NaN : (float) x[i]);
        return result;
    }

    /**
     * Creates a new array of floats holding the specified data. Values of {@code x} are converted to their
     * corresponding float values. Nulls are mapped to Float.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static float[] toFloat(final long[] x) {
        if (x == null) {
            return null;
        }
        final float[] result = new float[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_LONG ? Float.NaN : (float) x[i]);
        return result;
    }

    /**
     * Creates a new array of floats holding the specified data. Values of {@code x} are converted to their
     * corresponding float values. Nulls are mapped to Float.NaN.
     *
     * @param x data
     * @param <T> type of the data in {@code x}
     * @return new float array holding {@code x}
     */
    public static <T extends Number> float[] toFloat(final T[] x) {
        if (x == null) {
            return null;
        }
        final float[] result = new float[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == null ? Float.NaN : x[i].floatValue());
        return result;
    }

    /**
     * Creates a new array of doubles holding the specified data. Values of {@code x} are converted to their
     * corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static double[] toDouble(final float[] x) {
        if (x == null) {
            return null;
        }
        final double[] result = new double[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_FLOAT ? Double.NaN : (double) x[i]);
        return result;
    }

    /**
     * Creates a new array of doubles holding the specified data. Values of {@code x} are converted to their
     * corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static double[] toDouble(final int[] x) {
        if (x == null) {
            return null;
        }
        final double[] result = new double[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_INT ? Double.NaN : (double) x[i]);
        return result;
    }

    /**
     * Creates a new array of doubles holding the specified data. Values of {@code x} are converted to their
     * corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @return new float array holding {@code x}
     */
    public static double[] toDouble(final long[] x) {
        if (x == null) {
            return null;
        }
        final double[] result = new double[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == NULL_LONG ? Double.NaN : (double) x[i]);
        return result;
    }

    /**
     * Creates a new array of doubles holding the specified data. Values of {@code x} are converted to their
     * corresponding double values. Nulls are mapped to Double.NaN.
     *
     * @param x data
     * @param <T> type of the data in {@code x}
     * @return new float array holding {@code x}
     */
    public static <T extends Number> double[] toDouble(final T[] x) {
        if (x == null) {
            return null;
        }
        final double[] result = new double[x.length];
        IntStream.range(0, x.length).forEach(i -> result[i] = x[i] == null ? Double.NaN : x[i].doubleValue());
        return result;
    }

    /**
     * Gets the {@link Color} array for this palette.
     *
     * @return Array of {@link Color} for this palette.
     */
    public static Color[] getNColors(final ColorPalette colorPalette, final int n) {
        Require.neqNull(colorPalette, "colorPalette");
        Require.gt(n, "n", 0);
        final Color[] colors = new Color[n];
        int counter = 0;
        while (counter < n) {
            colors[counter] = colorPalette.get(counter);
            ++counter;
        }
        return colors;
    }

    public static double minIgnoreNaN(final double... values) {
        double min = Double.NaN;

        if (values == null) {
            return min;
        }

        for (double value : values) {
            min = minIgnoreNaN(min, value);
        }

        return min;
    }

    public static double minIgnoreNaN(final double oldMin, final double value) {
        if (oldMin == QueryConstants.NULL_DOUBLE) {
            return value;
        }
        if (value == QueryConstants.NULL_DOUBLE) {
            return oldMin;
        }

        return Double.isNaN(oldMin) || value < oldMin ? value : oldMin;
    }

    public static double maxIgnoreNaN(final double... values) {
        double max = Double.NaN;

        if (values == null) {
            return max;
        }

        for (double value : values) {
            max = maxIgnoreNaN(max, value);
        }

        return max;
    }

    public static double maxIgnoreNaN(final double oldMin, final double value) {
        if (oldMin == QueryConstants.NULL_DOUBLE) {
            return value;
        }
        if (value == QueryConstants.NULL_DOUBLE) {
            return oldMin;
        }

        return Double.isNaN(oldMin) || value > oldMin ? value : oldMin;
    }

    public static float minIgnoreNaN(final float oldMin, final float value) {
        if (oldMin == QueryConstants.NULL_FLOAT) {
            return value;
        }
        if (value == QueryConstants.NULL_FLOAT) {
            return oldMin;
        }

        return Float.isNaN(oldMin) || value < oldMin ? value : oldMin;
    }

    public static float maxIgnoreNaN(final float oldMin, final float value) {
        if (oldMin == QueryConstants.NULL_FLOAT) {
            return value;
        }
        if (value == QueryConstants.NULL_FLOAT) {
            return oldMin;
        }

        return Float.isNaN(oldMin) || value > oldMin ? value : oldMin;
    }

    public static int minIgnoreNull(final int oldMin, final int value) {
        if (oldMin == QueryConstants.NULL_INT) {
            return value;
        }
        if (value == QueryConstants.NULL_INT) {
            return oldMin;
        }

        return Math.min(value, oldMin);
    }

    public static int maxIgnoreNull(final int oldMin, final int value) {
        if (oldMin == QueryConstants.NULL_INT) {
            return value;
        }
        if (value == QueryConstants.NULL_INT) {
            return oldMin;
        }

        return Math.max(value, oldMin);
    }

    public static short minIgnoreNull(final short oldMin, final short value) {
        if (oldMin == QueryConstants.NULL_SHORT) {
            return value;
        }
        if (value == QueryConstants.NULL_SHORT) {
            return oldMin;
        }

        return value < oldMin ? value : oldMin;
    }

    public static short maxIgnoreNull(final short oldMin, final short value) {
        if (oldMin == QueryConstants.NULL_SHORT) {
            return value;
        }
        if (value == QueryConstants.NULL_SHORT) {
            return oldMin;
        }

        return value > oldMin ? value : oldMin;
    }

    public static long minIgnoreNull(final long oldMin, final long value) {
        if (oldMin == QueryConstants.NULL_LONG) {
            return value;
        }
        if (value == QueryConstants.NULL_LONG) {
            return oldMin;
        }

        return Math.min(value, oldMin);
    }

    public static long maxIgnoreNull(final long oldMin, final long value) {
        if (oldMin == QueryConstants.NULL_LONG) {
            return value;
        }

        if (value == QueryConstants.NULL_LONG) {
            return oldMin;
        }

        return Math.max(value, oldMin);
    }

    public static TableHandle createCategoryTableHandle(Table t, final String catColumn, final String... otherColumns) {
        return createCategoryTableHandle(t, new String[] {catColumn}, otherColumns);
    }

    public static TableHandle createCategoryTableHandle(Table t, final String[] catColumns,
            final String... otherColumns) {
        t = createCategoryTable(t, catColumns);

        final String[] cols = new String[otherColumns.length + catColumns.length + 1];
        System.arraycopy(catColumns, 0, cols, 0, catColumns.length);
        System.arraycopy(otherColumns, 0, cols, catColumns.length, otherColumns.length);
        cols[cols.length - 1] = CategoryDataSeries.CAT_SERIES_ORDER_COLUMN;

        return new TableHandle(t, cols);
    }

    public static TableBackedPartitionedTableHandle createCategoryPartitionedTableHandle(Table t,
            final String catColumn,
            final String[] otherColumns,
            final String[] byColumns,
            final PlotInfo plotInfo) {
        return createCategoryPartitionedTableHandle(t, new String[] {catColumn}, otherColumns, byColumns, plotInfo);
    }

    public static TableBackedPartitionedTableHandle createCategoryPartitionedTableHandle(Table t,
            final String[] catColumns,
            final String[] otherColumns,
            final String[] byColumns,
            final PlotInfo plotInfo) {
        final String[] lastByColumns = new String[catColumns.length + byColumns.length];
        System.arraycopy(catColumns, 0, lastByColumns, 0, catColumns.length);
        System.arraycopy(byColumns, 0, lastByColumns, catColumns.length, byColumns.length);

        t = createCategoryTable(t, lastByColumns);

        final Set<String> columns = new HashSet<>();
        Collections.addAll(columns, otherColumns);
        Collections.addAll(columns, lastByColumns);
        columns.add(CategoryDataSeries.CAT_SERIES_ORDER_COLUMN);

        return new TableBackedPartitionedTableHandle(t, columns, byColumns, plotInfo);
    }

    public static Table createCategoryTable(final Table t, final String[] catColumns) {
        // We need to do the equivalent of LastBy wrt. to columns included, or we have a chance to break ACLs
        final List<String> lastColumns = t.getDefinition().getColumnNames();
        lastColumns.removeAll(Arrays.asList(catColumns));
        final QueryTable result = (QueryTable) t.aggBy(
                createCategoryAggs(AggLast(lastColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))),
                ColumnName.from(catColumns));

        // We must explicitly copy attributes because we are doing a modified manual first/lastBy which will not
        // automatically do the copy.
        ((BaseTable<?>) t).copyAttributes(result, BaseTable.CopyAttributeOperation.LastBy);
        return result;
    }

    public static Table createCategoryHistogramTable(final Table t, final String... byColumns) {
        return t.aggBy(createCategoryAggs(AggCount(IntervalXYDataSeriesArray.COUNT)), ColumnName.from(byColumns));

    }

    public static Collection<? extends Aggregation> createCategoryAggs(Aggregation agg) {
        return List.of(Aggregation.AggFirstRowKey(CategoryDataSeries.CAT_SERIES_ORDER_COLUMN), agg);
    }

    public static List<Condition> getColumnConditions(final Table arg, final String column) {
        return Collections.singletonList(Condition.EQUALS);
    }

    public static Function<Long, Number> getNumberFromNumericOrTimeSource(final Table t, final String numericCol,
            final PlotInfo plotInfo) {
        ArgumentValidations.isNumericOrTime(t, numericCol, plotInfo);

        final ColumnSource columnSource = t.getColumnSource(numericCol);

        if (columnSource.getType() == DateTime.class) {
            return key -> {
                final DateTime dateTime = (DateTime) columnSource.get(key);
                return dateTime == null ? NULL_LONG : dateTime.getNanos();
            };
        } else if (columnSource.getType() == Date.class) {
            return key -> {
                final Date dateTime = (Date) columnSource.get(key);
                return dateTime == null ? NULL_LONG : dateTime.getTime() * 1000000;
            };
        } else {
            return key -> (Number) columnSource.get(key);
        }
    }

    public static class HashMapWithDefault<K, V> extends HashMap<K, V> {
        private V def = null;

        public HashMapWithDefault() {
            super();
        }

        private HashMapWithDefault(final HashMapWithDefault<K, V> map) {
            super(map);
            this.def = map.def;
        }

        public <T> void runIfKeyExistsCast(final Consumer<T> consumer, final K key) {
            final V obj = get(key);
            if (obj != null) {
                consumer.accept((T) obj);
            }
        }

        public void setDefault(V def) {
            this.def = def;
        }

        public V getDefault() {
            return def;
        }

        @Override
        public V get(Object key) {
            return super.getOrDefault(key, def);
        }

        public HashMapWithDefault<K, V> copy() {
            return new HashMapWithDefault<>(this);
        }
    }

    public static <T> IndexableData createIndexableData(final Table t, final String column, final PlotInfo plotInfo) {
        final DataColumn<T> dataColumn = t.getColumn(column);
        final Object o = dataColumn.getDirect();

        return new IndexableDataArray((T[]) o, plotInfo);
    }

    public static <T> IndexableData createIndexableData(final Map<String, Object> snapshotData,
            @NotNull final TableDefinition tableDefinition, final String column, final PlotInfo plotInfo) {
        return createIndexableData(snapshotData, tableDefinition.getColumn(column).getDataType(), column, plotInfo);
    }

    public static <T> IndexableData createIndexableData(final Map<String, Object> snapshotData, final Class<T> c,
            final String column, final PlotInfo plotInfo) {
        if (snapshotData == null) {
            return createEmptyIndexableData(c, plotInfo);
        }

        return createIndexableData(snapshotData.get(column), c, plotInfo);
    }

    public static <T> IndexableData createEmptyIndexableData(final Class<T> c, final PlotInfo plotInfo) {
        if (c.equals(int.class)) {
            return new IndexableDataInteger(EMPTY_INT_ARRAY, plotInfo);
        } else if (c.equals(double.class)) {
            return new IndexableDataDouble(EMPTY_DOUBLE_ARRAY, false, plotInfo);
        } else if (c.equals(float.class)) {
            return new IndexableDataDouble(EMPTY_FLOAT_ARRAY, false, plotInfo);
        } else if (c.equals(long.class)) {
            return new IndexableDataDouble(EMPTY_LONG_ARRAY, false, plotInfo);
        } else if (c.equals(short.class)) {
            return new IndexableDataDouble(EMPTY_SHORT_ARRAY, false, plotInfo);
        } else if (c.equals(char.class)) {
            return new IndexableDataCharacter(EMPTY_CHAR_ARRAY, plotInfo);
        } else if (c.equals(byte.class)) {
            return new IndexableDataByte(EMPTY_BYTE_ARRAY, plotInfo);
        }

        return new IndexableDataListNullCategory<T>(Collections.emptyList(), plotInfo);
    }

    public static <T> IndexableData createIndexableData(final Object data, final PlotInfo plotInfo) {
        if (data instanceof int[]) {
            return new IndexableDataInteger((int[]) data, plotInfo);
        } else if (data instanceof double[]) {
            return new IndexableDataDouble((double[]) data, false, plotInfo);
        } else if (data instanceof float[]) {
            return new IndexableDataDouble((float[]) data, false, plotInfo);
        } else if (data instanceof long[]) {
            return new IndexableDataDouble((long[]) data, false, plotInfo);
        } else if (data instanceof short[]) {
            return new IndexableDataDouble((short[]) data, false, plotInfo);
        } else if (data instanceof char[]) {
            return new IndexableDataCharacter((char[]) data, plotInfo);
        } else if (data instanceof byte[]) {
            return new IndexableDataByte((byte[]) data, plotInfo);
        }

        return new IndexableDataArrayNullCategory((T[]) data, plotInfo);
    }

    public static <T> IndexableData createIndexableData(final Object data, final Class<T> c, final PlotInfo plotInfo) {
        if (c.equals(int.class)) {
            return new IndexableDataInteger((int[]) data, plotInfo);
        } else if (c.equals(double.class)) {
            return new IndexableDataDouble((double[]) data, false, plotInfo);
        } else if (c.equals(float.class)) {
            return new IndexableDataDouble((float[]) data, false, plotInfo);
        } else if (c.equals(long.class)) {
            return new IndexableDataDouble((long[]) data, false, plotInfo);
        } else if (c.equals(short.class)) {
            return new IndexableDataDouble((short[]) data, false, plotInfo);
        } else if (c.equals(char.class)) {
            return new IndexableDataCharacter((char[]) data, plotInfo);
        } else if (c.equals(byte.class)) {
            return new IndexableDataByte((byte[]) data, plotInfo);
        } else if (c.equals(DateTime.class)) {
            if (data instanceof long[]) {
                return new IndexableDataDateTime((long[]) data, plotInfo);
            }
        }

        return new IndexableDataArrayNullCategory((T[]) data, plotInfo);
    }

    public static <T extends Comparable> IndexableData createIndexableData(final T[] data, final PlotInfo plotInfo) {
        return new IndexableDataArray(data, plotInfo);
    }

    public static IndexableNumericData createIndexableNumericDataArray(final Map<String, Object> data,
            @NotNull final TableHandle th, final String column, final PlotInfo plotInfo) {
        return createIndexableNumericDataArray(data, th.getTable(), column, plotInfo);
    }

    public static IndexableNumericData createIndexableNumericDataArray(final Map<String, Object> data,
            @NotNull final Table t, final String column, final PlotInfo plotInfo) {
        return createIndexableNumericDataArray(data, t.getDefinition(), column, plotInfo);
    }

    public static IndexableNumericData createIndexableNumericDataArray(final Map<String, Object> data,
            @NotNull final TableDefinition tableDefinition, final String column, final PlotInfo plotInfo) {
        if (data == null) {
            if (tableDefinition.getColumn(column) == null) {
                return createEmptyIndexableNumericDataArray(double.class, plotInfo);
            }

            return createEmptyIndexableNumericDataArray(tableDefinition.getColumn(column).getDataType(), plotInfo);
        }

        return createIndexableNumericDataArray(data.get(column), tableDefinition.getColumn(column).getDataType(),
                plotInfo);
    }

    public static IndexableNumericData createEmptyIndexableNumericDataArray(final Class dataType,
            final PlotInfo plotInfo) {
        if (dataType == int.class) {
            return new IndexableNumericDataArrayInt(EMPTY_INT_ARRAY, plotInfo);
        } else if (dataType == double.class) {
            return new IndexableNumericDataArrayDouble(EMPTY_DOUBLE_ARRAY, plotInfo);
        } else if (dataType == float.class) {
            return new IndexableNumericDataArrayFloat(EMPTY_FLOAT_ARRAY, plotInfo);
        } else if (dataType == long.class) {
            return new IndexableNumericDataArrayLong(EMPTY_LONG_ARRAY, plotInfo);
        } else if (dataType == short.class) {
            return new IndexableNumericDataArrayShort(EMPTY_SHORT_ARRAY, plotInfo);
        } else if (dataType == DateTime.class) {
            return new IndexableNumericDataArrayDateTime(EMPTY_DATETIME_ARRAY, plotInfo);
        } else if (dataType == Date.class) {
            return new IndexableNumericDataArrayDate(EMPTY_DATE_ARRAY, plotInfo);
        } else if (Number.class.isAssignableFrom(dataType)) {
            return new IndexableNumericDataArrayNumber(EMPTY_NUMBER_ARRAY, plotInfo);
        } else {
            throw new UnsupportedOperationException("Can not create IndexableNumericDataArray from type " + dataType);
        }
    }

    public static IndexableNumericData createIndexableNumericDataArray(final Object data, final Class dataType,
            final PlotInfo plotInfo) {
        if (dataType == int.class) {
            return new IndexableNumericDataArrayInt((int[]) data, plotInfo);
        } else if (dataType == double.class) {
            return new IndexableNumericDataArrayDouble((double[]) data, plotInfo);
        } else if (dataType == float.class) {
            return new IndexableNumericDataArrayFloat((float[]) data, plotInfo);
        } else if (dataType == long.class) {
            return new IndexableNumericDataArrayLong((long[]) data, plotInfo);
        } else if (dataType == short.class) {
            return new IndexableNumericDataArrayShort((short[]) data, plotInfo);
        } else if (dataType == DateTime.class) {
            if (data instanceof long[]) {
                return new IndexableNumericDataArrayLong((long[]) data, plotInfo);
            } else {
                return new IndexableNumericDataArrayDateTime((DateTime[]) data, plotInfo);
            }
        } else if (dataType == Date.class) {
            return new IndexableNumericDataArrayDate((Date[]) data, plotInfo);
        } else if (Number.class.isAssignableFrom(dataType)) {
            return new IndexableNumericDataArrayNumber((Number[]) data, plotInfo);
        } else {
            throw new UnsupportedOperationException("Can not create IndexableNumericDataArray from type " + dataType);
        }
    }
}
