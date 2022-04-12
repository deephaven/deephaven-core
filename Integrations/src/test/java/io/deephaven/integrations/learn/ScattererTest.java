package io.deephaven.integrations.learn;

import io.deephaven.engine.table.impl.InMemoryTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.function.Function;

public class ScattererTest {

    private static InMemoryTable table;


    @BeforeClass
    public static void setup() {
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

    @SafeVarargs
    private static Output[] createOutputs(Function<Object[], Object>... scatterFuncs) {
        return new Output[] {new Output("OutCol1", scatterFuncs[0], "int"),
                new Output("OutCol2", scatterFuncs[1], null)};
    }

    @SafeVarargs
    private static Output[] createOutputsPy(Function<Object[], Object>... scatterFuncs) {
        return new Output[] {new Output("OutCol1", true, scatterFuncs[0], "int"),
                new Output("OutCol2", true, scatterFuncs[1], null)};
    }

    private static Output[] createOutputs(Function<Object[], Object> scatterFunc) {
        return createOutputs(scatterFunc, scatterFunc);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullOutputArrayTest() {
        new Scatterer(null);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullOutputElementTest() {
        Output[] outputs = createOutputs(args -> args);
        new Scatterer(new Output[] {outputs[0], null});
    }

    @Test
    public void scatterMethodTest() {

        int batchSize = 7;

        Function<Object[], Object> gatherFunc = (params) -> 1;

        Function<Object[], Object> modelFunc = (params) -> 2;

        Function<Object[], Object> scatterFunc1 = (params) -> {

            // first parameter of scatterFunc is the result of modelFunc, so
            Assert.assertEquals(2, params[0]);
            // second parameter is the index of this particular output
            Assert.assertEquals(0, params[1]);

            return 3;
        };

        Function<Object[], Object> scatterFunc2 = (params) -> {

            // first parameter of scatterFunc is the result of modelFunc, so
            Assert.assertEquals(2, params[0]);
            // second parameter is the index of this particular output
            Assert.assertEquals(1, params[1]);

            return 4;
        };

        Input[] inputs = createInputs(gatherFunc);
        Output[] outputs = createOutputs(scatterFunc1, scatterFunc2);

        Computer computer = new Computer(table, modelFunc, inputs, batchSize);
        Scatterer scatterer = new Scatterer(outputs);

        FutureOffset[] futureOffsetColumn = new FutureOffset[9];


        for (int i = 0; i < 9; i++) {
            futureOffsetColumn[i] = computer.compute(i);
        }

        for (int i = 0; i < outputs.length; i++) {
            Assert.assertEquals((i == 0) ? 3 : 4, scatterer.scatter(i, futureOffsetColumn[i]));
        }
    }

    @Test
    public void generateQueryStringsTest() {

        Function<Object[], Object> scatterFunc1 = (params) -> 3;

        Function<Object[], Object> scatterFunc2 = (params) -> 4;

        Output[] outputs = createOutputs(scatterFunc1, scatterFunc2);

        Scatterer scatterer = new Scatterer(outputs);

        Assert.assertArrayEquals(
                new String[] {"OutCol1 = (int) (__scatterer.scatter(0, __FutureOffset))",
                        "OutCol2 =  (__scatterer.scatter(1, __FutureOffset))"},
                scatterer.generateQueryStrings("__FutureOffset"));
    }

    @Test
    public void generateQueryStringsPyTest() {

        Function<Object[], Object> scatterFunc1 = (params) -> 3;

        Function<Object[], Object> scatterFunc2 = (params) -> 4;

        Output[] outputs = createOutputsPy(scatterFunc1, scatterFunc2);

        Scatterer scatterer = new Scatterer(outputs);

        Assert.assertArrayEquals(
                new String[] {"OutCol1 = (int) (PyObject)  (__scatterer.scatter(0, __FutureOffset))",
                        "OutCol2 =  (PyObject)  (__scatterer.scatter(1, __FutureOffset))"},
                scatterer.generateQueryStrings("__FutureOffset"));
    }
}
