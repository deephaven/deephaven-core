/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class JettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration config = init(args, Main.class);
        final JettyConfig jettyConfig = JettyConfig.buildFromConfig(config).build();
        DaggerJettyServerComponent
                .builder()
                .withJettyConfig(jettyConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run()
                .join();
    }
}
