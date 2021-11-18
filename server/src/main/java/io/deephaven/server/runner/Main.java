package io.deephaven.server.runner;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.LogBufferInterceptor;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeoutException;

public class Main {
    private static void bootstrapSystemProperties(String[] args) throws IOException {
        if (args.length > 1) {
            throw new IllegalArgumentException("Expected 0 or 1 argument");
        }
        if (args.length == 0) {
            try (final InputStream in = Main.class.getResourceAsStream("/bootstrap.properties")) {
                if (in != null) {
                    System.out.println("# Bootstrapping from resource '/bootstrap.properties'%n");
                    System.getProperties().load(in);
                } else {
                    System.out.println("# No resource '/bootstrap.properties' found, skipping bootstrapping%n");
                }
            }
        } else {
            System.out.printf("# Bootstrapping from file '%s'%n", args[0]);
            try (final FileReader reader = new FileReader(args[0])) {
                System.getProperties().load(reader);
            }
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, TimeoutException {
        System.out.printf("# Starting %s%n", Main.class.getName());

        // No classes should be loaded before we bootstrap additional system properties
        bootstrapSystemProperties(args);

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

        // defaults to 5 minutes
        int httpSessionExpireMs = config.getIntegerWithDefault("http.session.durationMs", 300000);
        int httpPort = config.getIntegerWithDefault("http.port", 8080);
        int schedulerPoolSize = config.getIntegerWithDefault("scheduler.poolSize", 4);

        DaggerDeephavenApiServerComponent
                .builder()
                .withPort(httpPort)
                .withSchedulerPoolSize(schedulerPoolSize)
                .withSessionTokenExpireTmMs(httpSessionExpireMs)
                .withOut(PrintStreamGlobals.getOut())
                .withErr(PrintStreamGlobals.getErr())
                .build()
                .getServer()
                .run();
    }
}
