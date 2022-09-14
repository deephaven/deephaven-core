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
import io.deephaven.ssl.config.*;
import io.deephaven.ssl.config.SSLConfig.Builder;
import io.deephaven.ssl.config.SSLConfig.ClientAuth;
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
    public static final String SSL_CLIENT_AUTH = "ssl.clientAuthentication";
    public static final String PRIVATEKEY = "privatekey";
    public static final String CERTS = "certs";

    private static void bootstrapSystemProperties(String[] args) throws IOException {
        if (args.length > 1) {
            throw new IllegalArgumentException("Expected 0 or 1 argument");
        }
        if (args.length == 0) {
            try (final InputStream in = Main.class.getResourceAsStream("/bootstrap.properties")) {
                if (in != null) {
                    System.out.println("# Bootstrapping from resource '/bootstrap.properties'");
                    System.getProperties().load(in);
                } else {
                    System.out.println("# No resource '/bootstrap.properties' found, skipping bootstrapping");
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
     * {@value SSL_IDENTITY_PRIVATE_KEY_PATH}, {@value SSL_TRUST_TYPE}, {@value SSL_TRUST_PATH}, and
     * {@value SSL_CLIENT_AUTH}. Currently, the only valid identity type is {@value PRIVATEKEY}, and the only valid
     * trust type is {@value CERTS}. If no identity is present, empty will be returned.
     *
     * @param config the config
     * @return the optional SSL config
     */
    public static Optional<SSLConfig> parseSSLConfig(Configuration config) {
        return parseSSLConfig(null, config, true);
    }

    /**
     * Parser the same properties as {@link #parseSSLConfig(Configuration)}, except with property keys prefixed with
     * "outbound.". Identity configuration is not required.
     *
     * @param config the config
     * @return the optional outbound SSL config
     */
    public static Optional<SSLConfig> parseOutboundSSLConfig(Configuration config) {
        return parseSSLConfig("outbound.", config, false);
    }

    private static Optional<SSLConfig> parseSSLConfig(String prefix, Configuration config, boolean needsIdentity) {
        final Optional<Identity> identity = parseIdentityConfig(prefix, config);
        if (needsIdentity && identity.isEmpty()) {
            return Optional.empty();
        }
        final Builder builder = SSLConfig.builder();
        identity.ifPresent(builder::identity);
        parseTrustConfig(prefix, config).ifPresent(builder::trust);
        parseClientAuth(prefix, config).ifPresent(builder::clientAuthentication);
        return Optional.of(builder.build());
    }

    private static Optional<Identity> parseIdentityConfig(String prefix, Configuration config) {
        final String identityType = config.getStringWithDefault(prefix(prefix, SSL_IDENTITY_TYPE), null);
        if (identityType == null) {
            return Optional.empty();
        }
        if (!PRIVATEKEY.equals(identityType)) {
            throw new IllegalArgumentException(
                    String.format("Only support `%s` identity type through Configuration", PRIVATEKEY));
        }
        final String identityCa = config.getStringWithDefault(prefix(prefix, SSL_IDENTITY_CERT_CHAIN_PATH), null);
        final String identityKey = config.getStringWithDefault(prefix(prefix, SSL_IDENTITY_PRIVATE_KEY_PATH), null);
        if (identityCa == null || identityKey == null) {
            throw new IllegalArgumentException(String.format("Must specify `%s` and `%s`",
                    prefix(prefix, SSL_IDENTITY_CERT_CHAIN_PATH), prefix(prefix, SSL_IDENTITY_PRIVATE_KEY_PATH)));
        }
        return Optional.of(IdentityPrivateKey.builder().certChainPath(identityCa).privateKeyPath(identityKey).build());
    }

    private static Optional<? extends Trust> parseTrustConfig(String prefix, Configuration config) {
        final String trustType = config.getStringWithDefault(prefix(prefix, SSL_TRUST_TYPE), null);
        if (trustType == null) {
            return Optional.empty();
        }
        if (!CERTS.equals(trustType)) {
            throw new IllegalArgumentException(
                    String.format("Only support `%s` trust type through Configuration", CERTS));
        }
        final String trustPath = config.getStringWithDefault(prefix(prefix, SSL_TRUST_PATH), null);
        return Optional.ofNullable(trustPath)
                .map(TrustCertificates::of)
                .or(() -> {
                    throw new IllegalArgumentException(
                            String.format("Must specify `%s`", prefix(prefix, SSL_TRUST_PATH)));
                });
    }

    private static Optional<ClientAuth> parseClientAuth(String prefix, Configuration config) {
        final String clientAuth = config.getStringWithDefault(prefix(prefix, SSL_CLIENT_AUTH), null);
        return Optional.ofNullable(clientAuth).map(ClientAuth::valueOf);
    }

    private static String prefix(String prefix, String key) {
        return prefix != null ? prefix + key : key;
    }
}
