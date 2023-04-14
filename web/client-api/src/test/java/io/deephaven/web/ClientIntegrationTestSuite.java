package io.deephaven.web;

import com.google.gwt.junit.tools.GWTTestSuite;
import io.deephaven.web.client.api.filter.FilterConditionTestGwt;
import junit.framework.Test;
import junit.framework.TestSuite;

public class ClientIntegrationTestSuite extends GWTTestSuite {
    public static Test suite() {
        TestSuite suite = new TestSuite("Deephaven JS API Unit Test Suite");

        // This test doesn't actually talk to the server, but it requires the dh-internal library be available
        suite.addTestSuite(FilterConditionTestGwt.class);



        return suite;
    }
}
