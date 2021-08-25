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
            fail(message + " expected: " + FilterPrinter.print(output) + " but was: "
                + FilterPrinter.print(actual));
        }
        assertEquals(message, output, actual);
    }

    protected abstract Condition execute(Condition f);
}
