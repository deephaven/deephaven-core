//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.custom;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.MainHelper;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * An example of a "custom integrator" main using a {@link CustomComponentFactory}.
 */
public final class CustomMain {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration configuration = MainHelper.init(args, CustomMain.class);
        new CustomComponentFactory()
                .build(configuration)
                .getServer()
                .run()
                .join();
    }
}
