package io.deephaven.integrations.learn;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.InMemoryTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class FutureTest {

    private static InMemoryTable table;

    @BeforeClass
    public static void createTable() {
        table = new InMemoryTable(
                new String[] {"Column1", "Column2", "Column3"},
                new Object[] {
                        new int[] {1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[] {2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[] {5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });

    }

    @SafeVarargs
    private static Input[] createInputs(Function<Object[], Object>... gatherFuncs) {
        return new Input[] {new Input(new String[] {"Column1", "Column2"}, gatherFuncs[0]),
                new Input("Column3", gatherFuncs[1])};
    }

    private static Input[] createInputs(Function<Object[], Object> gatherFunc) {
        return createInputs(gatherFunc, gatherFunc);
    }

    private static Future createFuture(Function<Object[], Object> modelFunc, Input[] inputs, int batchSize) {
        return new Future(modelFunc, inputs,
                new ColumnSource[][] {
                        table.view("Column1", "Column2").getColumnSources()
                                .toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY),
                        table.view("Column3").getColumnSources()
                                .toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY)},
                batchSize);
    }

    @Test
    public void getMethodTest() {

        final Input[] inputs = createInputs(args -> args);

        final int batchSize = 7;
        final RowSet[] rowSetTarget = new RowSet[1];
        final ColumnSource<?>[][] colSourceTarget = new ColumnSource[inputs.length][];

        for (int i = 0; i < inputs.length; i++) {
            colSourceTarget[i] = inputs[i].createColumnSource(table);
        }

        Function<Object[], Object> myGather1 = (params) -> {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(rowSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[] {colSourceTarget[0]}[0], params[1]));

            return 4;
        };

        Function<Object[], Object> myGather2 = (params) -> {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(rowSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[] {colSourceTarget[1]}[0], params[1]));

            return 5;
        };

        Function<Object[], Object> modelFunc = (params) -> 3;

        Input[] thisInput = createInputs(myGather1, myGather2);

        Future future = createFuture(modelFunc, thisInput, batchSize);

        try (final WritableRowSet rowSet = RowSetFactory.empty()) {
            for (int i = 0; i < 3; i++) {
                rowSet.insert(i);
                future.addRowKey(i);
            }

            rowSetTarget[0] = rowSet;
            Assert.assertEquals(3, future.get());
            rowSetTarget[0] = null;
        }
    }

    @Test
    public void gatherMethodTest() {

        final Input[] inputs = createInputs(args -> args);

        final int batchSize = 7;
        final RowSet[] rowSetTarget = new RowSet[1];
        final ColumnSource<?>[][] colSourceTarget = new ColumnSource[inputs.length][];

        for (int i = 0; i < inputs.length; i++) {
            colSourceTarget[i] = inputs[i].createColumnSource(table);
        }

        Function<Object[], Object> myGather1 = (params) -> {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(rowSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[] {colSourceTarget[0]}[0], params[1]));

            return 10;
        };

        Function<Object[], Object> myGather2 = (params) -> {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(rowSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[] {colSourceTarget[1]}[0], params[1]));

            return 11;
        };

        Input[] thisInput = createInputs(myGather1, myGather2);

        Function<Object[], Object> modelFunc = (params) -> 3;

        Future future = createFuture(modelFunc, thisInput, batchSize);

        try (final WritableRowSet rowSet = RowSetFactory.empty()) {
            for (int i = 0; i < 3; i++) {
                rowSet.insert(i);
                future.addRowKey(i);
            }

            rowSetTarget[0] = rowSet;

            for (int i = 0; i < thisInput.length; i++) {
                Assert.assertEquals((i == 0) ? 10 : 11,
                        future.gather(thisInput[i], thisInput[i].createColumnSource(table), rowSet));
            }

            rowSetTarget[0] = null;
        }
    }

    @Test
    public void getIndexSetTest() {

        int batchSize = 7;

        Input[] thisInput = createInputs(args -> args);

        Function<Object[], Object> modelFunc = (params) -> 3;

        for (int i = 0; i < batchSize; i++) {
            getIndexSetTest(modelFunc, thisInput, batchSize, i);
        }
    }

    public void getIndexSetTest(final Function<Object[], Object> modelFunc, final Input[] thisInput,
            final int batchSize, final int n) {
        Future future = createFuture(modelFunc, thisInput, batchSize);

        try (final WritableRowSet rowSetTarget = RowSetFactory.empty()) {
            for (int i = 0; i < n; i++) {
                rowSetTarget.insert(i);
                future.addRowKey(i);
            }

            try (final RowSet rowSet = future.makeRowSet()) {
                Assert.assertEquals(rowSetTarget, rowSet);
            }
        }
    }
}
