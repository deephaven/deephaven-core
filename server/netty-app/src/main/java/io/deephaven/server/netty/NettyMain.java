/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.MainHelper;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @deprecated see io.deephaven.server.jetty.JettyMain
 */
@Deprecated
public final class NettyMain {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        final Configuration configuration = MainHelper.init(args, NettyMain.class);
        new DeprecatedCommunityComponentFactory()
                .build(configuration)
                .getServer()
                .run()
                .join();
    }
}
