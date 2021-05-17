package io.deephaven.grpc_api.table.ops.filter;

import io.deephaven.proto.backplane.grpc.Condition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractNormalizingFilterTest {
    protected void assertUnchanged(String message, Condition input) {
        assertEquals(message, input, execute(input));
    }

    protected void assertFilterEquals(String message, Condition input, Condition output) {

        Condition actual = execute(input);
        if (!output.equals(actual)) {
            fail(message + " expected: " + print(output) + " but was: " + print(actual));
        }
        assertEquals(message, output, actual);
    }

    private static String print(Condition f) {
        FilterPrinter p = new FilterPrinter(str -> "\"" + str + "\"");//not correct, but good enough for logging failures
        return p.print(f);
    }

    protected abstract Condition execute(Condition f);
}
