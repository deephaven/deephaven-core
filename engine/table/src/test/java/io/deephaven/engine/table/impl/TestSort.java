/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.exceptions.NotSortableException;
import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.util.ColumnHolder;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class TestSort extends BaseArrayTestCase {

    private static final boolean ENABLE_QUERY_COMPILER_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(TestSort.class, "QueryCompiler.logEnabled", false);

    private boolean lastMemoize = false;
    private boolean oldQueryCompilerLogEnabled;
    private SafeCloseable executionContext;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
        lastMemoize = QueryTable.setMemoizeResults(false);
        oldQueryCompilerLogEnabled = QueryCompiler.setLogEnabled(ENABLE_QUERY_COMPILER_LOGGING);
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        QueryCompiler.setLogEnabled(oldQueryCompilerLogEnabled);
        QueryTable.setMemoizeResults(lastMemoize);
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
        executionContext.close();
    }

    @FunctionalInterface
    interface ThrowingConsumer<A, T extends Exception> {
        void consume(A val) throws T;
    }

    private <T extends Exception> void assertException(QueryTable t, ThrowingConsumer<QueryTable, T> r,
            String failMessage, Class<T> excType) {
        try {
            r.consume(t);
            fail(failMessage);
        } catch (Exception e) {
            assertTrue(e.getClass().toString() + " is not a " + excType.toString(), excType.isInstance(e));
        }
    }

    public void testSortMulti() {
        for (int ncols = 1; ncols <= 4; ++ncols) {
            for (int size = 1; size <= 32768; size *= 2) {
                sortMultiTester(ncols, size, false, new StringGenerator(2));
                sortMultiTester(ncols, size, true, new StringGenerator(2));
            }
        }
    }

    public void testSortTypes() {
        for (int ncols = 1; ncols <= 2; ++ncols) {
            for (int size = 1024; size <= 8192; size *= 2) {
                sortTypeTester(ncols, size, new DoubleGenerator(100, 0.01));
                sortTypeTester(ncols, size, new FloatGenerator(100, 0.01));
                sortTypeTester(ncols, size, new ByteGenerator((byte) 256));
                sortTypeTester(ncols, size, new ShortGenerator((short) 1000));
                sortTypeTester(ncols, size, new IntGenerator(1000));
                sortTypeTester(ncols, size, new LongGenerator(1000));
            }
        }
    }

    public void testRestrictedSortingwhere() {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(1000));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");

        assertException(source, (t) -> t.assertSortable(t.getDefinition().getColumnNamesArray()),
                "Columns 1 and 3 should not be sortable.", NotSortableException.class);

        source.assertSortable("Column1", "Column3");
        QueryTable temp = (QueryTable) source.sort("Column3");

        assertException(temp, (t) -> t.sort("Column2"), "Column2 should not be sortable", NotSortableException.class);
        assertException(source, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        // Check where()
        temp = (QueryTable) source.where("Column2 > 24");
        temp.sort("Column3");
        temp.sort("Column1");
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = (QueryTable) temp.clearSortingRestrictions();
        temp.assertSortable(temp.getDefinition().getColumnNamesArray());
        temp.sort(temp.getDefinition().getColumnNamesArray());

        assertException(source, (t) -> t.assertSortable(t.getDefinition().getColumnNamesArray()),
                "Columns 1 and 3 should not be sortable.", NotSortableException.class);

        temp = (QueryTable) temp.clearSortingRestrictions();
        temp.assertSortable(temp.getDefinition().getColumnNamesArray());
    }

    public void testRestrictedSortingSelect() {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(10));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");

        // Check Select
        QueryTable temp = (QueryTable) source.select();
        assertException(temp, (t) -> t.sort(t.getDefinition().getColumnNamesArray()),
                "Columns 0 and 2 should not be sortable.", NotSortableException.class);

        testRestrictedSortingViewSelect((t, a) -> (QueryTable) (a == null || a.length <= 0 ? t.select() : t.select(a)));
    }

    public void testRestrictSortingView() {
        testRestrictedSortingViewSelect((t, a) -> (QueryTable) t.view(a));
    }

    private void testRestrictedSortingViewSelect(BiFunction<Table, String[], QueryTable> func) {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(1000));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");

        QueryTable temp = func.apply(source, new String[] {"Column1a=Column1", "Column0=Column3", "Column2=Column2",
                "Column5=Column3", "Column5=Column1"});
        temp.sort("Column1a");
        temp.sort("Column5");
        temp.sort("Column0");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NoSuchColumnException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column3=Column3+Column1", "Column2=Column2",
                "Column5=Column3", "Column5=Column0"});
        temp.sort("Column1a");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column5"), "Should not be able to sort by Column5",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column3=Column3+Column1", "Column2=Column2",
                "Column5=Column3", "Column5=Column1"});
        temp.sort("Column1a");
        temp.sort("Column5");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column0=Column3", "Column2=Column2",
                "Column5=Column3", "Column5=Column1", "Column1a=Column3+Column0"});
        assertException(temp, (t) -> t.sortDescending("Column1a"), "Should not be able to sort by Column1a",
                NotSortableException.class);
        temp.sort("Column5");
        temp.sort("Column0");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NoSuchColumnException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);
    }

    public void testRestrictedSortingUpdate() {
        testRestrictedSortingUpdateUpdateView((t, a) -> (QueryTable) t.update(a));
    }

    public void testRestrictedSortingUpdateView() {
        testRestrictedSortingUpdateUpdateView((t, a) -> (QueryTable) t.updateView(a));
    }

    private void testRestrictedSortingUpdateUpdateView(BiFunction<Table, String[], QueryTable> func) {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(1000));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");

        QueryTable temp = func.apply(source, new String[] {"Column1a=Column1", "Column0=Column3", "Column2=Column2",
                "Column5=Column3", "Column5=Column1"});
        temp.sort("Column1a");
        temp.sort("Column5");
        temp.sort("Column0");
        temp.sortDescending("Column1");
        temp.sortDescending("Column3");
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column3=Column3+Column1", "Column2=Column2",
                "Column5=Column3", "Column5=Column0"});
        temp.sort("Column1a");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column5"), "Should not be able to sort by Column5",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column3=Column3+Column1", "Column2=Column2",
                "Column5=Column3", "Column5=Column1"});
        temp.sort("Column1a");
        temp.sort("Column5");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        temp = func.apply(source, new String[] {"Column1a=Column1", "Column0=Column3", "Column2=Column2",
                "Column5=Column3", "Column5=Column1", "Column1a=Column3+Column0"});
        assertException(temp, (t) -> t.sortDescending("Column1a"), "Should not be able to sort by Column1a",
                NotSortableException.class);
        temp.sort("Column5");
        temp.sort("Column0");
        temp.sortDescending("Column3");
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column1a"), "Should not be able to sort by Column1a",
                NotSortableException.class);
    }

    public void testRestrictedSortingDropColumns() {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(1000));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");

        QueryTable temp = (QueryTable) source.dropColumns("Column3");
        temp.sort("Column1");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NoSuchColumnException.class);
        temp = (QueryTable) temp.update("Column3=Column0");
        temp.sort("Column1");
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NotSortableException.class);

        temp = (QueryTable) temp.restrictSortTo("Column3");
        temp.sort("Column3");
        assertException(temp, (t) -> t.sortDescending("Column1"), "Should not be able to sort by Column1",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column0"), "Should not be able to sort by Column0",
                NotSortableException.class);
    }

    public void testRestrictedSortingRenameColumns() {
        final QueryTable source = generateSortTesterTable(4, 1024, new IntGenerator(1000));

        // All columns should be sortable
        source.assertSortable(source.getDefinition().getColumnNamesArray());
        source.restrictSortTo("Column1", "Column3");
        source.sort("Column1");
        source.sort("Column3");
        assertException(source, (t) -> t.sortDescending("Column0"), "Should not be able to sort by Column0",
                NotSortableException.class);
        assertException(source, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);

        QueryTable temp = (QueryTable) source.renameColumns("Column0a=Column0", "Column1a=Column1", "Column2a=Column2",
                "Column3a=Column3");
        temp.sort("Column1a");
        temp.sort("Column3a");
        assertException(temp, (t) -> t.sortDescending("Column0a"), "Should not be able to sort by Column0a",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2a"), "Should not be able to sort by Column2a",
                NotSortableException.class);

        assertException(temp, (t) -> t.sortDescending("Column0"), "Should not be able to sort by Column0",
                NoSuchColumnException.class);
        assertException(temp, (t) -> t.sortDescending("Column1"), "Should not be able to sort by Column1",
                NoSuchColumnException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NoSuchColumnException.class);
        assertException(temp, (t) -> t.sortDescending("Column3"), "Should not be able to sort by Column3",
                NoSuchColumnException.class);

        temp = (QueryTable) temp.clearSortingRestrictions();
        temp.sort("Column0a");
        temp.sort("Column1a");
        temp.sort("Column2a");
        temp.sort("Column3a");

        temp = (QueryTable) source.renameColumns("Column0=Column0", "Column1=Column1", "Column2=Column2",
                "Column3=Column3");
        temp.sort("Column1");
        temp.sort("Column3");
        assertException(temp, (t) -> t.sortDescending("Column0"), "Should not be able to sort by Column0",
                NotSortableException.class);
        assertException(temp, (t) -> t.sortDescending("Column2"), "Should not be able to sort by Column2",
                NotSortableException.class);
    }

    private class MultiColumnSortHelper {
        private class Wrapper implements Comparable<Wrapper> {
            int getSentinel() {
                return sentinel;
            }

            int sentinel;
            int sign;

            Wrapper(int sentinel, boolean reverse) {
                this.sentinel = sentinel;
                this.sign = reverse ? -1 : 1;
            }

            public int compareTo(@NotNull Wrapper ww) {
                for (int ii = 0; ii < colsToUse; ++ii) {
                    Comparable comparable = columnData[ii][sentinel];
                    Comparable comparable1 = columnData[ii][ww.getSentinel()];
                    if (comparable == comparable1)
                        continue;
                    if (comparable == null)
                        return -1 * sign;
                    if (comparable1 == null)
                        return 1 * sign;
                    int res = comparable.compareTo(comparable1);
                    if (res != 0)
                        return sign * res;
                }
                return 0;
            }
        }

        Comparable[][] columnData;
        int colsToUse;
        Wrapper[] sorted;
        Wrapper[] reverseSorted;

        MultiColumnSortHelper(Comparable[][] columnData, int colsToUse) {
            this.columnData = columnData;
            this.colsToUse = colsToUse;
            sorted = new Wrapper[columnData[0].length];
            reverseSorted = new Wrapper[columnData[0].length];
            for (int ii = 0; ii < sorted.length; ++ii) {
                sorted[ii] = new Wrapper(ii, false);
                reverseSorted[ii] = new Wrapper(ii, true);
            }
            Arrays.sort(sorted);
            Arrays.sort(reverseSorted);
        }

        long getSentinel(int position) {
            return sorted[position].getSentinel() + 1;
        }

        long getReverseSentinel(int position) {
            return reverseSorted[position].getSentinel() + 1;
        }
    }

    private abstract class DataGenerator {
        protected static final double nullFraction = 0.01;

        public abstract Class getType();

        abstract Comparable makeEntry();

        abstract ColumnSource generateColumnSource(int size);
    }

    private class StringGenerator extends DataGenerator {
        final int wordLen;

        StringGenerator(int wordLen) {
            this.wordLen = wordLen;
        }

        public Class getType() {
            return String.class;
        }

        public String makeEntry() {
            final int max = (int) Math.pow(10, wordLen);
            return Integer.toString((int) (max * Math.random()));
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            String[] column = new String[size];
            for (int ii = 0; ii < size; ++ii) {
                column[ii] = makeEntry();
            }
            return ArrayBackedColumnSource.getMemoryColumnSourceUntyped(column);
        }
    }

    private class DoubleGenerator extends DataGenerator {
        final double range;
        final double round;

        DoubleGenerator(double range, double round) {
            this.range = range;
            this.round = round;
        }

        public Class getType() {
            return double.class;
        }

        public Double makeEntry() {
            if (Math.random() < nullFraction)
                return null;
            return Math.rint((Math.random() * range) / round) * round;
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            double[] column = new double[size];
            for (int ii = 0; ii < size; ++ii) {
                Double value = makeEntry();
                if (value == null)
                    column[ii] = QueryConstants.NULL_DOUBLE;
                else
                    column[ii] = value;
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private class FloatGenerator extends DataGenerator {
        final double range;
        final double round;

        FloatGenerator(double range, double round) {
            this.range = range;
            this.round = round;
        }

        public Class getType() {
            return float.class;
        }

        public Float makeEntry() {
            return (float) (Math.rint((Math.random() * range) / round) * round);
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            float[] column = new float[size];
            for (int ii = 0; ii < size; ++ii) {
                column[ii] = makeEntry();
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private class DateTimeGenerator extends DataGenerator {
        public Class getType() {
            return DateTime.class;
        }

        public DateTime makeEntry() {
            if (Math.random() < nullFraction) {
                return null;
            }

            long startTime = 1385063840; // Thu Nov 21 14:57:20 2013
            long offset = (int) Math.rint(Math.random() * 3600);
            offset *= 1000000000;

            DateTime dateTime = new DateTime((startTime * 1000000000) - offset);
            return dateTime;
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            throw new UnsupportedOperationException();
        }
    }

    private class ByteGenerator extends DataGenerator {
        final byte range;

        ByteGenerator(byte range) {
            this.range = range;
        }

        public Class getType() {
            return byte.class;
        }

        public Byte makeEntry() {
            return (byte) Math.round(Math.random() * range);
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            byte[] column = new byte[size];
            for (int ii = 0; ii < size; ++ii) {
                column[ii] = makeEntry();
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private class ShortGenerator extends DataGenerator {
        final short range;

        ShortGenerator(short range) {
            this.range = range;
        }

        public Class getType() {
            return short.class;
        }

        public Short makeEntry() {
            return (short) Math.round(Math.random() * range);
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            short[] column = new short[size];
            for (int ii = 0; ii < size; ++ii) {
                column[ii] = makeEntry();
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private class IntGenerator extends DataGenerator {
        final int range;

        IntGenerator(int range) {
            this.range = range;
        }

        public Class getType() {
            return int.class;
        }

        public Integer makeEntry() {
            if (Math.random() < nullFraction)
                return null;
            return (int) Math.round(Math.random() * range);
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            int[] column = new int[size];
            for (int ii = 0; ii < size; ++ii) {
                Integer value = makeEntry();
                if (value == null)
                    column[ii] = QueryConstants.NULL_INT;
                else
                    column[ii] = value;
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private class LongGenerator extends DataGenerator {
        final long range;

        LongGenerator(long range) {
            this.range = range;
        }

        public Class getType() {
            return long.class;
        }

        public Long makeEntry() {
            return Math.round(Math.random() * range);
        }

        @Override
        ColumnSource generateColumnSource(int size) {
            long[] column = new long[size];
            for (int ii = 0; ii < size; ++ii) {
                column[ii] = makeEntry();
            }
            return ArrayBackedColumnSource.getMemoryColumnSource(column);
        }
    }

    private QueryTable generateSortTesterTable(int ncols, int size, DataGenerator dataGenerator) {
        Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        for (int ii = 0; ii < ncols; ++ii) {
            columns.put("Column" + ii, dataGenerator.generateColumnSource(size));
        }

        Integer[] sentinels = new Integer[size];
        for (int jj = 0; jj < size; ++jj) {
            sentinels[jj] = jj + 1;
        }
        columns.put("Sentinel", ArrayBackedColumnSource.getMemoryColumnSourceUntyped(sentinels));

        return new QueryTable(RowSetFactory.fromRange(0, size - 1).toTracking(), columns);
    }

    private Comparable[][] createBoxedData(Table source, int ncols, int size) {
        final Comparable[][] boxedData = new Comparable[ncols][];
        for (int ii = 0; ii < ncols; ++ii) {
            final DataColumn column = source.getColumn("Column" + ii);
            boxedData[ii] = new Comparable[size];
            for (int jj = 0; jj < size; ++jj) {
                boxedData[ii][jj] = (Comparable) column.get(jj);
            }
        }

        return boxedData;
    }

    private void sortTypeTester(int ncols, int size, DataGenerator dataGenerator) {
        System.out.println("Sorting table of size " + size + " with " + ncols + " columns.");

        final Table source = generateSortTesterTable(ncols, size, dataGenerator);
        final Comparable[][] boxedData = createBoxedData(source, ncols, size);

        sortTester(ncols, size, boxedData, source);
    }

    private void sortMultiTester(int ncols, int size, boolean grouped, StringGenerator dataGenerator) {
        System.out.println("Sorting table of size " + size + " with " + ncols + " columns.");

        ColumnHolder columnHolders[] = new ColumnHolder[ncols + 1];
        Comparable[][] boxedData = new Comparable[ncols][];

        for (int ii = 0; ii < ncols; ++ii) {
            String[] data = new String[size];
            for (int jj = 0; jj < size; jj++) {
                data[jj] = dataGenerator.makeEntry();
            }
            columnHolders[ii] = new ColumnHolder<>("Column" + ii, String.class, null, grouped, data);
            boxedData[ii] = data;
        }

        Integer[] sequence = new Integer[size];
        for (int jj = 0; jj < size; jj++) {
            sequence[jj] = jj + 1;
        }
        columnHolders[ncols] = new ColumnHolder<>("Sentinel", Integer.class, null, false, sequence);

        Table source = TableTools.newTable(columnHolders);

        sortTester(ncols, size, boxedData, source);
    }

    private void sortTester(int ncols, int size, Comparable[][] columnData, Table source) {
        sortTester(ncols, size, columnData, source, false);
        sortTester(ncols, size, columnData, source, true);
    }

    private void sortTester(int ncols, int size, Comparable[][] columnData, Table source, boolean isRefreshing) {
        ((QueryTable) source).setRefreshing(isRefreshing);

        // Now sort the table by the sentinel, which should just give us a simple ordering.
        assertEquals(source.size(), size);
        assertEquals(source.getColumn("Sentinel").size(), size);

        Table result0 = source.sort("Sentinel");
        // show(result0);
        DataColumn col = result0.getColumn("Sentinel");
        assertEquals(col.size(), size);
        for (int jj = 0; jj < size; ++jj) {
            assertEquals(jj + 1, col.get(jj));
        }

        Table result1 = source.sortDescending("Sentinel");
        // show(result1);
        col = result1.getColumn("Sentinel");
        assertEquals(col.size(), size);
        for (int jj = 0; jj < size; ++jj) {
            assertEquals(size - jj, col.get(jj));
        }

        // Sort it by Column0 through (Column0, .. ColumnN-1)
        for (int ii = 1; ii <= ncols; ++ii) {
            String[] colNames = new String[ii];
            for (int jj = 0; jj < ii; ++jj) {
                colNames[jj] = "Column" + jj;
            }

            System.out.println("Sorted by " + Arrays.toString(colNames));
            Table resultAscending = source.sort(colNames);
            Table resultDescending = source.sortDescending(colNames);
            // TableTools.show(resultAscending);
            // TableTools.show(resultDescending);


            DataColumn colAscending = resultAscending.getColumn("Sentinel");
            assertEquals(colAscending.size(), size);
            DataColumn colDescending = resultDescending.getColumn("Sentinel");
            assertEquals(colDescending.size(), size);

            MultiColumnSortHelper multiColumnSortHelper = new MultiColumnSortHelper(columnData, ii);
            for (int jj = 0; jj < size; ++jj) {
                assertEquals(multiColumnSortHelper.getSentinel(jj), (int) colAscending.get(jj));
                assertEquals(multiColumnSortHelper.getReverseSentinel(jj), (int) colDescending.get(jj));
            }
        }
    }
}
