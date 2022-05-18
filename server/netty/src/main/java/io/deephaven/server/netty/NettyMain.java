package io.deephaven.server.netty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The Netty server main. Parses {@link NettyConfig} from the JSON file from the property {@value SERVER_CONFIG_PROP}.
 *
 * @see io.deephaven.server.netty
 * @see NettyConfig
 */
public class NettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {

        final NettyConfig nettyConfig =
                init(args, Main.class, NettyConfig::defaultConfig, NettyConfig::parseJsonUnchecked);

        DaggerNettyServerComponent
                .builder()
                .withNettyConfig(nettyConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
