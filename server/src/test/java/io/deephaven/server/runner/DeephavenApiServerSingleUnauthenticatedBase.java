package io.deephaven.server.runner;

import io.deephaven.grpc_api.DeephavenChannel;
import org.junit.Before;

public abstract class DeephavenApiServerSingleUnauthenticatedBase extends DeephavenApiServerTestBase {

    DeephavenChannel channel;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        channel = channel();
    }
}
