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
    private static Input[] inputs;

    @BeforeClass
    public static void createTable() {
        table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        inputs = new Input[]{new Input(new String[]{"Column1","Column2"}, args -> args), new Input("Column3", args -> args)};
    }

    static Input[] createInputs(Function<Object[], Object> gatherFunc) {
        return new Input[]{new Input(new String[]{"Column1","Column2"}, gatherFunc), new Input("Column3", gatherFunc)};
    }

    @Test
    public void computeMethodTest() {

        final Function<Object[], Object> func = args -> args;
        final int batchSize = 7;

        Computer computer = new Computer(table, func, inputs, batchSize);

        for (int i = 0 ; i < 9 ; i++) {
            computer.compute(i);
        }

        computer.clear();
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullTableTest() {

        final Function<Object[], Object> func = args -> args;
        final int batchSize = 7;

        Computer computer = new Computer(null, func, inputs, batchSize);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullFunctionTest() {

        final Function<Object[], Object> func = null;
        final int batchSize = 7;

        Computer computer = new Computer(table, func, inputs, batchSize);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void invalidBatchSizeTest() {

        final Function<Object[], Object> func = args -> args;
        final int batchSize = 0;

        Computer computer = new Computer(table, func, inputs, batchSize);
    }

    @Test
    public void computeGatherGetTest() {

        final int batchSize = 7;

        final IndexSet[] indexSetTarget = new IndexSet[1];
        ColumnSource<?>[][] colSourceTarget = new ColumnSource[inputs.length][];
        for (int i = 0 ; i < inputs.length ; i++) {
            colSourceTarget[i] = inputs[i].createColumnSource(table);
        }

        final int[] counter = {0};

        Function<Object[], Object> myGather = (params) ->
        {
            // first we test equality on IndexSet
            Assert.assertEquals(indexSetTarget[0], params[0]);

            // now we test equality on ColumnSources, which depends on which input object we are comparing
            if (counter[0] == 0) {

                Assert.assertTrue(Objects.deepEquals(new Object[]{colSourceTarget[0]}[0], params[1]));
                counter[0]++;

            } else {
                Assert.assertTrue(Objects.deepEquals(new Object[]{colSourceTarget[1]}[0], params[1]));
            }

            return 4;
        };

        Function<Object[], Object> myModel = (thisInput) ->
        {
            Assert.assertArrayEquals(new Object[]{4,4}, thisInput); // I have 2 Input objects in inputs, so gather gets called twice
            return 5;
        };

        Input[] thisInput = createInputs(myGather);

        Computer computer = new Computer(table, myModel, thisInput, batchSize);

        for (int i = 0 ; i < 9 ; i++) {
            computer.compute(i);
        }
        for (int i = 0 ; i < 9 ; i++) {
            indexSetTarget[0] = computer.getFuture().getIndexSet();
            Assert.assertEquals(5, computer.getFuture().get());
        }
        computer.clear();
        Assert.assertEquals(null, computer.getFuture());
    }
}