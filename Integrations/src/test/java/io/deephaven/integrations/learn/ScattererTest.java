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
                new ColumnSource[][]{table.select(colNames).getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY)},
                7);

        outputs = new Output[]{new Output("OutCol", func, "int")};
    }

    @Test
    public void scatterMethodTest() {

        Scatterer scatterer = new Scatterer(outputs);

        System.out.println(scatterer.scatter(0, new FutureOffset(future, 0)));

    }

    @Test
    public void generateQueryStringsTest() {

        Scatterer scatterer = new Scatterer(outputs);

        Assert.assertEquals("OutCol =  (scatterer.scatter(0, __FutureOffset))",
                scatterer.generateQueryStrings("__FutureOffset")[0]);
    }

}
