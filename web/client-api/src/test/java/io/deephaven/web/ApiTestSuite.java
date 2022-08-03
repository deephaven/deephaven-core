/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web;

import com.google.gwt.junit.tools.GWTTestSuite;
import io.deephaven.web.client.api.filter.FilterConditionTestGwt;
import io.deephaven.web.client.api.i18n.JsDateTimeFormatTestGwt;
import io.deephaven.web.client.api.i18n.JsNumberFormatTestGwt;
import junit.framework.Test;
import junit.framework.TestSuite;

public class ApiTestSuite extends GWTTestSuite {
    public static Test suite() {
        TestSuite suite = new TestSuite("Deephaven Web API Test Suite");
        suite.addTestSuite(FilterConditionTestGwt.class);

        suite.addTestSuite(JsDateTimeFormatTestGwt.class);
        suite.addTestSuite(JsNumberFormatTestGwt.class);
        return suite;
    }
}
