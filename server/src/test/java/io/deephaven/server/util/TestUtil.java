/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.FunctionalInterfaces;

public class TestUtil {
    public static <ExceptionType extends Exception> void assertThrows(
            final Class<ExceptionType> type, final FunctionalInterfaces.ThrowingRunnable<ExceptionType> runner) {
        boolean threwExpectedException = false;
        try {
            runner.run();
        } catch (final Exception exception) {
            threwExpectedException = type.isAssignableFrom(exception.getClass());
        }
        Assert.eqTrue(threwExpectedException, "threwExpectedException");
    }
}
