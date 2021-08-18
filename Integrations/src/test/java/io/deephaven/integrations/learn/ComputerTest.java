package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class ComputerTest {

    private static InMemoryTable table;

    @BeforeClass
    public static void createTable() {
        table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
    }

    private static Input[] createInputs(Function<Object[], Object>... gatherFuncs) {
        return new Input[]{new Input(new String[]{"Column1","Column2"}, gatherFuncs[0]), new Input("Column3", gatherFuncs[1])};
    }

    private static Input[] createInputs(Function<Object[], Object> gatherFunc) {
        return createInputs(gatherFunc, gatherFunc);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullTableTest() {

        final Input[] inputs = createInputs(args -> args);
        final int batchSize = 7;

        Function<Object[], Object> modelFunc = (params) ->
        {
            return 1;
        };

        Computer computer = new Computer(null, modelFunc, inputs, batchSize);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullFunctionTest() {

        final Input[] inputs = createInputs(args -> args);
        final int batchSize = 7;

        Function<Object[], Object> modelFunc = null;

        Computer computer = new Computer(table, modelFunc, inputs, batchSize);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void invalidBatchSizeTest() {

        final Input[] inputs = createInputs(args -> args);
        final int batchSize = 0;

        Function<Object[], Object> modelFunc = (params) ->
        {
            return 1;
        };

        Computer computer = new Computer(table, modelFunc, inputs, batchSize);
    }

    @Test
    public void computeGatherGetTest() {


        final Input[] inputs = createInputs(args -> args);

        // initialize necessary reference objects and batchSize parameter
        final int batchSize = 7;
        final IndexSet[] indexSetTarget = new IndexSet[1];
        final ColumnSource<?>[][] colSourceTarget = new ColumnSource[inputs.length][];

        // create columnSources based off of given Input and tables
        for (int i = 0 ; i < inputs.length ; i++) {
            colSourceTarget[i] = inputs[i].createColumnSource(table);
        }

        // define gather functions
        Function<Object[], Object> myGather1 = (params) ->
        {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(indexSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[]{colSourceTarget[0]}[0], params[1]));

            return 4;
        };

        Function<Object[], Object> myGather2 = (params) ->
        {
            Assert.assertEquals(2, params.length);
            Assert.assertEquals(indexSetTarget[0], params[0]);
            Assert.assertTrue(Objects.deepEquals(new Object[]{colSourceTarget[1]}[0], params[1]));

            return 5;
        };

        // define model function
        Function<Object[], Object> myModel = (params) ->
        {
            Assert.assertArrayEquals(new Object[]{4,5}, params);
            return 6;
        };


        Input[] thisInput = createInputs(myGather1, myGather2);

        Computer computer = new Computer(table, myModel, thisInput, batchSize);

        for (int i = 0 ; i < 9 ; i++) {
            computer.compute(i);
        }
        for (int i = 0 ; i < 9 ; i++) {
            indexSetTarget[0] = computer.getFuture().getIndexSet();
            Assert.assertEquals(6, computer.getFuture().get()); // computer.getFuture.get() triggers assertions in gather functions
        }
        computer.clear();
        Assert.assertNull(computer.getFuture());
    }
}