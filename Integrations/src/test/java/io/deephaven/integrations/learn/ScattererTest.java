package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.function.Function;

public class ScattererTest {

    private static Future future;
    private static Output[] outputs;


    @BeforeClass
    public static void setup() {
        InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        Function<Object[], Object> func = args -> args;
        String[] colNames = new String[]{"Column1","Column2"};
        future = new Future(func, new Input[]{new Input(colNames, func)},
                new ColumnSource[][]{table.view(colNames).getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY)},
                7);

        outputs = new Output[]{new Output("OutCol1", func, "int"),
                               new Output("OutCol2", func, null)};
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullOutputArrayTest() {
        Scatterer scatterer = new Scatterer(null);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullOutputElementTest() {
        Scatterer scatterer = new Scatterer(new Output[]{outputs[0], null});
    }

    @Test
    public void scatterMethodTest() {

        Scatterer scatterer = new Scatterer(outputs);

        System.out.println(scatterer.scatter(0, new FutureOffset(future, 0)));

    }

    @Test
    public void generateQueryStringsTest() {

        Scatterer scatterer = new Scatterer(outputs);

        Assert.assertArrayEquals(
                new String[]{"OutCol1 =  (__scatterer.scatter(0, __FutureOffset))",
                             "OutCol2 =  (__scatterer.scatter(1, __FutureOffset))"},
                scatterer.generateQueryStrings("__FutureOffset"));
    }

}
