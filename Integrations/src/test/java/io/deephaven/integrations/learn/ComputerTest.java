package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
    public void futureOffsetMethodsFromComputerTest() {

        final Function<Object[], Object> func = args -> args;
        final int batchSize = 7;

        Computer computer = new Computer(table, func, inputs, batchSize);

        FutureOffset fo = null;

        for (int i = 0 ; i < 9 ; i++) {
            fo = computer.compute(i);
            Assert.assertEquals(i % batchSize, fo.getOffset());
            Assert.assertEquals(computer.getFuture(), fo.getFuture());
            Assert.assertEquals((i % batchSize)+1, fo.getFuture().getIndexSet().getSize());
        }
    }
}