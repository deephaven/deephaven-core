package io.deephaven.integrations.learn;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class OutputTest {

    @Test
    public void createOutputTest1() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        Output output = new Output(colName, func, type);

        Assert.assertEquals(colName, output.getColName());
        Assert.assertEquals(func, output.getScatterFunc());
        Assert.assertEquals(type, output.getType());
    }

    @Test(expected = io.deephaven.db.tables.utils.NameValidator.InvalidNameException.class)
    public void createOutputTest2() {
        final String colName = " Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        Output output = new Output(colName, func, type);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void createOutputTest3() {
        final String colName = null;
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        Output output = new Output(colName, func, type);
    }

    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void createOutputTest4() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = null;
        final String type = "int";

        Output output = new Output(colName, func, type);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void createOutputTest5() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "hello";

        Output output = new Output(colName, func, type);
    }
}