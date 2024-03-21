//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.data;

import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.util.tables.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static io.deephaven.util.QueryConstants.*;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class TestIndexableData {

    @Rule
    final public EngineCleanup framework = new EngineCleanup();

    private final int SIZE = 200;
    private final short[] shortArray = new short[SIZE];
    private final int[] intArray = new int[SIZE];
    private final double[] doubleArray = new double[SIZE];
    private final long[] longArray = new long[SIZE];
    private final float[] floatArray = new float[SIZE];
    private final Number[] numberArray = new Number[SIZE];
    private final List<Double> numberList = new ArrayList<>(SIZE);
    private final Date[] dateArray = new Date[SIZE];
    private final Instant[] instantsArray = new Instant[SIZE];

    @Before
    public void setUp() {
        short i = 0;
        shortArray[i] = NULL_SHORT;
        intArray[i] = NULL_INT;
        doubleArray[i] = NULL_DOUBLE;
        longArray[i] = NULL_LONG;
        floatArray[i] = NULL_FLOAT;
        numberArray[i] = null;
        numberList.add(i, null);
        dateArray[i] = null;
        instantsArray[i] = null;

        for (i = 1; i < SIZE; i++) {
            shortArray[i] = i;
            intArray[i] = i;
            doubleArray[i] = i;
            longArray[i] = i;
            floatArray[i] = i;
            numberArray[i] = i;
            numberList.add(i, (double) i);
            dateArray[i] = new Date(i, 1, 1);
            instantsArray[i] = DateTimeUtils.epochNanosToInstant(i);
        }
    }

    @Test
    public void testIndexableNumericData() {
        final IndexableNumericData shortData = new IndexableNumericDataArrayShort(shortArray, null);
        final IndexableNumericData intData = new IndexableNumericDataArrayInt(intArray, null);
        final IndexableNumericData doubleData = new IndexableNumericDataArrayDouble(doubleArray, null);
        final IndexableNumericData longData = new IndexableNumericDataArrayLong(longArray, null);
        final IndexableNumericData floatData = new IndexableNumericDataArrayFloat(floatArray, null);
        final IndexableNumericData numberData = new IndexableNumericDataArrayNumber<>(numberArray, null);
        final IndexableNumericData listData = new IndexableNumericDataListNumber<>(numberList, null);
        final IndexableNumericData dateData = new IndexableNumericDataArrayDate(dateArray, null);
        final IndexableNumericData instantData = new IndexableNumericDataArrayInstant(instantsArray, null);
        checkData(shortData, intData, doubleData, longData, floatData, numberData, listData, instantData);
        checkDateData(dateData);
    }

    @Test
    public void testIndexableDouble() {
        IndexableData shortData = new IndexableDataDouble(shortArray, false, null);
        IndexableData intData = new IndexableDataDouble(intArray, false, null);
        IndexableData doubleData = new IndexableDataDouble(doubleArray, false, null);
        IndexableData longData = new IndexableDataDouble(longArray, false, null);
        IndexableData floatData = new IndexableDataDouble(floatArray, false, null);
        IndexableData numberData = new IndexableDataDouble(numberArray, false, null);
        checkData(Double.NaN, true, shortData, intData, doubleData, longData, floatData, numberData);

        shortData = new IndexableDataDouble(shortArray, true, null);
        intData = new IndexableDataDouble(intArray, true, null);
        doubleData = new IndexableDataDouble(doubleArray, true, null);
        longData = new IndexableDataDouble(longArray, true, null);
        floatData = new IndexableDataDouble(floatArray, true, null);
        numberData = new IndexableDataDouble(numberArray, true, null);
        checkData(null, true, shortData, intData, doubleData, longData, floatData, numberData);
    }

    @Test
    public void testIndexableInteger() {
        final IndexableData intData = new IndexableDataInteger(intArray, null);
        checkData(null, true, intData);
    }

    @Test
    public void testIndexableDataTable() {
        final Table t = TableTools.newTable(TableTools.shortCol("shortCol", shortArray),
                TableTools.intCol("intCol", intArray), TableTools.doubleCol("doubleCol", doubleArray),
                TableTools.floatCol("floatCol", floatArray), TableTools.longCol("longCol", longArray),
                TableTools.col("numberCol", numberArray));
        final BaseFigureImpl figure = new BaseFigureImpl();

        final TableHandle tableHandle =
                new TableHandle(t, "shortCol", "intCol", "doubleCol", "floatCol", "longCol", "numberCol");
        final ColumnHandlerFactory.ColumnHandler shortColumnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "shortCol", null);
        final ColumnHandlerFactory.ColumnHandler intColumnHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "intCol", null);
        final ColumnHandlerFactory.ColumnHandler doubleColHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "doubleCol", null);
        final ColumnHandlerFactory.ColumnHandler floatColHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "floatCol", null);
        final ColumnHandlerFactory.ColumnHandler longColHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "longCol", null);
        final ColumnHandlerFactory.ColumnHandler numberColHandler =
                ColumnHandlerFactory.newNumericHandler(tableHandle, "numberCol", null);

        final IndexableData shortData = new IndexableDataTable(shortColumnHandler, null);
        final IndexableData intData = new IndexableDataTable(intColumnHandler, null);
        final IndexableData doubleData = new IndexableDataTable(doubleColHandler, null);
        final IndexableData floatData = new IndexableDataTable(floatColHandler, null);
        final IndexableData longData = new IndexableDataTable(longColHandler, null);
        final IndexableData numberData = new IndexableDataTable(numberColHandler, null);

        checkData(null, false, shortData, intData, doubleData, floatData, longData, numberData);
    }

    @Test
    public void testIndexableDataInfinite() {
        final IndexableDataInfinite indexableDataInfinite =
                new IndexableDataInfinite<>(new IndexableDataDouble(doubleArray, true, null));
        assertEquals(Integer.MAX_VALUE, indexableDataInfinite.size());
        for (int i = 1; i < doubleArray.length; i++) {
            assertEquals(doubleArray[i], indexableDataInfinite.get(i));
        }

        assertNull(indexableDataInfinite.get(-1));
        assertNull(indexableDataInfinite.get(0));
        assertNull(indexableDataInfinite.get(doubleArray.length + 1));
    }

    @Test
    public void testIndexableDataWithDefault() {
        final IndexableDataWithDefault indexableDataWithDefault = new IndexableDataWithDefault(null);

        indexableDataWithDefault.setSpecific(new IndexableDataDouble(doubleArray, false, null), false);
        assertEquals(doubleArray.length, indexableDataWithDefault.size());
        for (int i = 1; i < doubleArray.length; i++) {
            assertEquals(doubleArray[i], indexableDataWithDefault.get(i));
        }

        indexableDataWithDefault.setSpecific(new IndexableDataDouble(doubleArray, false, null), false);
        assertEquals(doubleArray.length, indexableDataWithDefault.size());
    }

    private void checkData(Double emptyValue, boolean checkOutOfBounds, IndexableData... datasets) {
        for (IndexableData dataset : datasets) {
            assertEquals(dataset.get(0), emptyValue);
            Class c = dataset.get(199).getClass();
            if (c.equals(double.class) || c.equals(Double.class)) {
                assertEquals(dataset.get(199), 199.0);
            } else if (c.equals(short.class) || c.equals(Short.class)) {
                assertEquals(dataset.get(199), (short) 199);
            } else if (c.equals(int.class) || c.equals(Integer.class)) {
                assertEquals(dataset.get(199), 199);
            } else if (c.equals(float.class) || c.equals(Float.class)) {
                assertEquals(dataset.get(199), 199.0f);
            } else if (c.equals(long.class) || c.equals(Long.class)) {
                assertEquals(dataset.get(199), 199L);
            } else {
                assertEquals(dataset.get(199), 199.0);
            }

            if (checkOutOfBounds) {
                if (!(dataset instanceof IndexableDataDouble) ||
                        ((IndexableDataDouble) dataset).getMapNanToNull()) {
                    assertNull(dataset.get(SIZE));
                } else {
                    assertEquals(Double.NaN, dataset.get(SIZE));
                }
            }
        }
    }

    private void checkData(IndexableNumericData... datasets) {
        for (IndexableNumericData dataset : datasets) {
            assertEquals(SIZE, dataset.size());
            assertEquals(dataset.get(0), Double.NaN);
            assertEquals(dataset.get(199), 199.0, 0.01);
            assertEquals(Double.NaN, dataset.get(SIZE));
        }
    }

    private void checkDateData(IndexableNumericData... datasets) {
        for (IndexableNumericData dataset : datasets) {
            assertEquals(dataset.get(0), Double.NaN);
            assertEquals(dataset.get(199), new Date(199, 1, 1).getTime() * 1000000, 1E12);
            assertEquals(Double.NaN, dataset.get(SIZE));
        }
    }

    @Test
    public void testDoubleStream() {
        final double[] data = {1, 2, 3, 4};
        final double target = Arrays.stream(data).sum();
        final IndexableNumericData doubleData = new IndexableNumericDataArrayDouble(data, null);
        final double actual = doubleData.stream().sum();

        assertEquals(target, actual);
    }
}
