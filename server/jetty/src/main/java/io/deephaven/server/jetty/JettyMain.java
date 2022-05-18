package io.deephaven.server.jetty;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.server.runner.Main;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The Jetty server main. Parses {@link JettyConfig} from the JSON file from the property {@value SERVER_CONFIG_PROP}.
 *
 * @see io.deephaven.server.jetty
 * @see JettyConfig
 */
public class JettyMain extends Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {

        final JettyConfig jettyConfig =
                init(args, Main.class, JettyConfig::defaultConfig, JettyConfig::parseJsonUnchecked);

        DaggerJettyServerComponent
                .builder()
                .withJettyConfig(jettyConfig)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
