package io.deephaven.grpc_api.runner;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.LogBufferInterceptor;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.internal.log.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;

public class Main {
    private static void bootstrapSystemProperties() throws IOException {
        try (final InputStream in = Main.class.getResourceAsStream("/bootstrap.properties")) {
            if (in != null) {
                System.getProperties().load(in);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.printf("# Starting %s%n", Main.class.getName());

        // No classes should be loaded before we bootstrap additional system properties
        bootstrapSystemProperties();

        // Capture the original System.out and System.err early
        PrintStreamGlobals.init();

        // Since our dagger injection happens later, we need to provider a static way to get the LogBuffer (for example,
        // logback configuration may reference LogBufferAppender).
        LogBufferGlobal.setInstance(new LogBufferInterceptor(Integer.getInteger("logBuffer.history", 1024)));

        final Logger log = LoggerFactory.getLogger(Main.class);

        log.info().append("Starting up ").append(Main.class.getName()).append("...").endl();

        final Configuration config = Configuration.getInstance();

        // Push our log to ProcessEnvironment, so that any parts of the system relying on ProcessEnvironment
        // instead of LoggerFactory can get the correct logger.
        final ProcessEnvironment processEnvironment =
                ProcessEnvironment.basicInteractiveProcessInitialization(config, Main.class.getName(), log);
        Thread.setDefaultUncaughtExceptionHandler(processEnvironment.getFatalErrorReporter());

        DeephavenApiServer.startMain(PrintStreamGlobals.getOut(), PrintStreamGlobals.getErr());
    }
}
