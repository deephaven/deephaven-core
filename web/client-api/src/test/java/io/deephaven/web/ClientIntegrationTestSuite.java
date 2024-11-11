//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web;

import com.google.gwt.junit.tools.GWTTestSuite;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.storage.JsStorageServiceTestGwt;
import io.deephaven.web.client.api.subscription.ConcurrentTableTestGwt;
import io.deephaven.web.client.api.subscription.ViewportTestGwt;
import io.deephaven.web.client.fu.LazyPromiseTestGwt;
import junit.framework.Test;
import junit.framework.TestSuite;

public class ClientIntegrationTestSuite extends GWTTestSuite {
    public static Test suite() {
        TestSuite suite = new TestSuite("Deephaven JS API Integration Test Suite");

        // This test doesn't actually talk to the server, but it requires the dh-internal library be available.
        // Disabled for now, we don't have good toString on the FilterCondition/FilterValue types.
        // suite.addTestSuite(FilterConditionTestGwt.class);

        // Actual integration tests
        suite.addTestSuite(ViewportTestGwt.class);
        suite.addTestSuite(TableManipulationTestGwt.class);
        suite.addTestSuite(ConcurrentTableTestGwt.class);
        suite.addTestSuite(NullValueTestGwt.class);
        suite.addTestSuite(HierarchicalTableTestGwt.class);
        suite.addTestSuite(PartitionedTableTestGwt.class);
        suite.addTestSuite(JsStorageServiceTestGwt.class);
        suite.addTestSuite(InputTableTestGwt.class);
        suite.addTestSuite(ColumnStatisticsTestGwt.class);

        // This should be a unit test, but it requires a browser environment to run on GWT 2.9
        // GWT 2.9 doesn't have proper bindings for Promises in HtmlUnit, so we need to use the IntegrationTest suite
        // for these tests.
        suite.addTestSuite(LazyPromiseTestGwt.class);

        // Unfinished:
        // suite.addTestSuite(TotalsTableTestGwt.class);

        return suite;
    }
}
