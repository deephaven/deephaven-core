package io.deephaven.integrations.learn;

import io.deephaven.api.util.NameValidator;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class OutputTest {

    @Test
    public void verifyCorrectFieldsTest() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        Output output = new Output(colName, func, type);

        Assert.assertEquals(colName, output.getColName());
        Assert.assertEquals(func, output.getScatterFunc());
        Assert.assertEquals(type, output.getType());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void verifyCorrectFieldsNullTypeTest() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = null;

        Output output = new Output(colName, func, type);

        Assert.assertEquals(colName, output.getColName());
        Assert.assertEquals(func, output.getScatterFunc());
        Assert.assertEquals(type, output.getType());
    }

    @Test(expected = NameValidator.InvalidNameException.class)
    public void invalidColumnNameTest() {
        final String colName = " Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        new Output(colName, func, type);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullColumnNameTest() {
        final String colName = null;
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        new Output(colName, func, type);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = io.deephaven.base.verify.RequirementFailure.class)
    public void nullFunctionTest() {
        final String colName = "Outcol";
        final Function<Object[], Object> func = null;
        final String type = "int";

        new Output(colName, func, type);
    }

    @Test
    public void toStringTest() {

        final String colName = "Outcol";
        final Function<Object[], Object> func = args -> args;
        final String type = "int";

        Output output = new Output(colName, func, type);

        Assert.assertEquals("Output{" +
                "colName='" + colName + '\'' +
                ", isPythonScatterFunc=" + output.isPythonScatterFunc() +
                ", scatterFunc=" + func +
                ", type='" + type + '\'' +
                '}', output.toString());
    }
}
