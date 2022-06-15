/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.io.logger.LogBufferInterceptor;
import io.deephaven.io.logger.Logger;
import io.deephaven.ssl.config.Identity;
import io.deephaven.ssl.config.IdentityPrivateKey;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.SSLConfig.Builder;
import io.deephaven.ssl.config.SSLConfig.ClientAuth;
import io.deephaven.ssl.config.Trust;
import io.deephaven.ssl.config.TrustCertificates;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class Main {

    public static final String SSL_IDENTITY_TYPE = "ssl.identity.type";
    public static final String SSL_IDENTITY_CERT_CHAIN_PATH = "ssl.identity.certChainPath";
    public static final String SSL_IDENTITY_PRIVATE_KEY_PATH = "ssl.identity.privateKeyPath";
    public static final String SSL_TRUST_TYPE = "ssl.trust.type";
    public static final String SSL_TRUST_PATH = "ssl.trust.path";
    public static final String PRIVATEKEY = "privatekey";
    public static final String CERTS = "certs";

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

    /**
     * Parses the configuration properties {@value SSL_IDENTITY_TYPE}, {@value SSL_IDENTITY_CERT_CHAIN_PATH},
     * {@value SSL_IDENTITY_PRIVATE_KEY_PATH}, {@value SSL_TRUST_TYPE}, and {@value SSL_TRUST_PATH}. Currently, the only
     * valid identity type is {@value PRIVATEKEY}, and the only valid trust type is {@value CERTS}.
     *
     * @param config the config
     * @return the optional SSL config
     */
    public static Optional<SSLConfig> parseSSLConfig(Configuration config) {
        final Optional<Identity> identity = parseIdentityConfig(config);
        final Optional<Trust> trust = parseTrustConfig(config);
        if (identity.isEmpty() && trust.isPresent()) {
            throw new IllegalArgumentException("Can't configure mTLS without server identity");
        }
        final Builder builder = SSLConfig.builder();
        identity.ifPresent(builder::identity);
        trust.ifPresent(builder::trust);
        // Note: it should be valid to configure ClientAuth.WANTED and mTLS - in which case, users who don't present
        // an mTLS identity can still get access with unauthenticated privileges. But this limited configuration does
        // not support that right now.
        trust.ifPresent(t -> builder.clientAuthentication(ClientAuth.NEEDED));
        return Optional.of(builder.build());
    }

    private static Optional<Identity> parseIdentityConfig(Configuration config) {
        final String identityType = config.getStringWithDefault(SSL_IDENTITY_TYPE, null);
        if (identityType == null) {
            return Optional.empty();
        }
        if (!PRIVATEKEY.equals(identityType)) {
            throw new IllegalArgumentException(
                    String.format("Only support `%s` identity type through Configuration", PRIVATEKEY));
        }
        final String identityCa = config.getStringWithDefault(SSL_IDENTITY_CERT_CHAIN_PATH, null);
        final String identityKey = config.getStringWithDefault(SSL_IDENTITY_PRIVATE_KEY_PATH, null);
        if (identityCa == null || identityKey == null) {
            throw new IllegalArgumentException(String.format("Must specify `%s` and `%s`", SSL_IDENTITY_CERT_CHAIN_PATH,
                    SSL_IDENTITY_PRIVATE_KEY_PATH));
        }
        return Optional.of(IdentityPrivateKey.builder().certChainPath(identityCa).privateKeyPath(identityKey).build());
    }

    private static Optional<Trust> parseTrustConfig(Configuration config) {
        final String trustType = config.getStringWithDefault(SSL_TRUST_TYPE, null);
        if (trustType == null) {
            return Optional.empty();
        }
        if (!CERTS.equals(trustType)) {
            throw new IllegalArgumentException(
                    String.format("Only support `%s` trust type through Configuration", CERTS));
        }
        final String trustPath = config.getStringWithDefault(SSL_TRUST_PATH, null);
        if (trustPath == null) {
            throw new IllegalArgumentException(String.format("Must specify `%s`", SSL_TRUST_PATH));
        }
        return Optional.of(TrustCertificates.of(trustPath));
    }
}
