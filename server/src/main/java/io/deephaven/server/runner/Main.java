package io.deephaven.server.runner;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.LogBufferInterceptor;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

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

    /**
     * Common init method to share between main() implementations.
     * 
     * @param mainClass
     * @return the current configuration instance to be used when configuring the rest of the server
     * @throws IOException
     */
    @NotNull
    public static Configuration init(String[] args, Class<?> mainClass) throws IOException {
        System.out.printf("# Starting %s%n", mainClass.getName());

        // No classes should be loaded before we bootstrap additional system properties
        bootstrapSystemProperties(args);

        // Capture the original System.out and System.err early
        PrintStreamGlobals.init();

        // Since our dagger injection happens later, we need to provider a static way to get the LogBuffer (for example,
        // logback configuration may reference LogBufferAppender).
        LogBufferGlobal.setInstance(new LogBufferInterceptor(Integer.getInteger("logBuffer.history", 1024)));

        final Logger log = LoggerFactory.getLogger(mainClass);

        log.info().append("Starting up ").append(mainClass.getName()).append("...").endl();

        final Configuration config = Configuration.getInstance();

        // After logging and config are working, redirect any future JUL logging to SLF4J
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        // Push our log to ProcessEnvironment, so that any parts of the system relying on ProcessEnvironment
        // instead of LoggerFactory can get the correct logger.
        final ProcessEnvironment processEnvironment =
                ProcessEnvironment.basicInteractiveProcessInitialization(config, mainClass.getName(), log);
        Thread.setDefaultUncaughtExceptionHandler(processEnvironment.getFatalErrorReporter());
        return config;
    }
}
