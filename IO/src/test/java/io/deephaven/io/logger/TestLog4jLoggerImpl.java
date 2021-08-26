/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

public class TestLog4jLoggerImpl extends TestCase {

    private Mockery context;
    private org.apache.log4j.Logger log4jlogger;

    public void setUp() throws Exception {
        super.setUp();

        context = new Mockery();
        context.setImposteriser(ClassImposteriser.INSTANCE);
        log4jlogger = context.mock(org.apache.log4j.Logger.class);
    }

    // TODO: this is NOT a test of the Log4jLogger class, it's just enough so I can watch it in the debugger once.

    public void testSimple() {
        Log4jLoggerImpl SUT = new Log4jLoggerImpl(log4jlogger);
        context.checking(new Expectations() {
            {
                one(log4jlogger).isEnabledFor(Level.INFO);
                will(returnValue(true));
                one(log4jlogger).log(Level.INFO, "foobar", null);
            }
        });
        SUT.info().append("foo").append("bar").endl();
        context.assertIsSatisfied();
    }
}
