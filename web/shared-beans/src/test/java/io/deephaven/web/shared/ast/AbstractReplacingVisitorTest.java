package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractReplacingVisitorTest {
    protected void assertUnchanged(String message, FilterDescriptor input) {
        assertEquals(message, input, execute(input));
    }

    protected void assertFilterEquals(String message, FilterDescriptor input,
        FilterDescriptor output) {

        FilterDescriptor actual = execute(input);
        if (!output.equals(actual)) {
            fail(message + " expected: " + print(output) + " but was: " + print(actual));
        }
        assertEquals(message, output, actual);
    }

    private static String print(FilterDescriptor f) {
        FilterPrinter p = new FilterPrinter(str -> "\"" + str + "\"");// not correct, but good
                                                                      // enough for logging failures
        return p.print(f);
    }

    protected abstract FilterDescriptor execute(FilterDescriptor f);
}
