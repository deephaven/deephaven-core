package io.deephaven.integrations.learn;

import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.InMemoryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class InputTest {

    @Test
    public void createInputTest1() {
        final InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        final String[] colNames = new String[]{"Column1", "Column2"};
        final Function<Object[], Object> func = args -> args;

        Input input = new Input(colNames, func);

        Assert.assertEquals(func, input.getGatherFunc());
        Assert.assertArrayEquals(table.select(colNames).getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY), input.createColumnSource(table));
    }

    @Test(expected = io.deephaven.db.tables.utils.NameValidator.InvalidNameException.class)
    public void createInputTest2() {
        final InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        final String[] colNames = new String[]{"Column1", null};
        final Function<Object[], Object> func = args -> args;

        Input input = new Input(colNames, func);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void createInputTest3() {
        final InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        final String[] colNames = null;
        final Function<Object[], Object> func = args -> args;

        Input input = new Input(colNames, func);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void createInputTest4() {
        final InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        final String[] colNames = new String[]{"Column1", "Column2"};
        final Function<Object[], Object> func = null;

        Input input = new Input(colNames, func);
    }

    @Test(expected = NameValidator.InvalidNameException.class)
    public void createInputTest5() {
        final InMemoryTable table = new InMemoryTable(
                new String[]{"Column1", "Column2", "Column3"},
                new Object[]{
                        new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4},
                        new long[]{2L, 4L, 2L, 4L, 6L, 2L, 4L, 6L, 8L},
                        new double[]{5.1, 2.8, 5.7, 2.4, 7.5, 2.2, 6.4, 2.1, 7.8}
                });
        final String[] colNames = new String[]{" Column1", "Column2"};
        final Function<Object[], Object> func = args -> args;

        Input input = new Input(colNames, func);
    }
}