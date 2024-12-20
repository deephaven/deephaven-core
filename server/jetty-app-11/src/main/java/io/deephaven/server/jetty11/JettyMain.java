//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.MainHelper;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The out-of-the-box Deephaven community server.
 *
 * @see CommunityComponentFactory
 */
public final class JettyMain {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration configuration = MainHelper.init(args, JettyMain.class);
        new CommunityComponentFactory()
                .build(configuration)
                .getServer()
                .run()
                .join();
    }
}
