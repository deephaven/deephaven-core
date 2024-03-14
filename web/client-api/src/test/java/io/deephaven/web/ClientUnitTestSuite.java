//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web;

import com.google.gwt.junit.tools.GWTTestSuite;
import io.deephaven.web.client.api.i18n.JsDateTimeFormatTestGwt;
import io.deephaven.web.client.api.i18n.JsNumberFormatTestGwt;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests that require a browser environment to run, but do not require the server.
 */
public class ClientUnitTestSuite extends GWTTestSuite {
    public static Test suite() {
        TestSuite suite = new TestSuite("Deephaven JS API Unit Test Suite");
        suite.addTestSuite(JsDateTimeFormatTestGwt.class);
        suite.addTestSuite(JsNumberFormatTestGwt.class);
        return suite;
    }
}
