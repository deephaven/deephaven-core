//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import io.grpc.ManagedChannel;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * A client channel factory that may update {@link ClientConfig} values before handing off to a {@link #delegate()}.
 */
@Immutable
@BuildableStyle
public abstract class ClientChannelFactoryDefaulter implements ClientChannelFactory {

    public static Builder builder() {
        return ImmutableClientChannelFactoryDefaulter.builder();
    }

    /**
     * The delegated client channel factory. By default, is {@link ClientChannelFactory#defaultInstance()}.
     */
    @Default
    public ClientChannelFactory delegate() {
        return ClientChannelFactory.defaultInstance();
    }

    /**
     * The ssl config. This will set {@link ClientConfig#ssl() ssl} for the {@code config} in
     * {@link #create(ClientConfig)} if it has not already been set and the {@link ClientConfig#target() target} is
     * {@link DeephavenTarget#isSecure() secure}.
     */
    public abstract Optional<SSLConfig> ssl();

    /**
     * The user-agent. This will set {@link ClientConfig#userAgent()} for the {@code config} in
     * {@link #create(ClientConfig)} if it has not already been set.
     */
    public abstract Optional<String> userAgent();

    /**
     * Creates a managed channel. Will update {@code config} with the defaults as specified by {@link #ssl()} and
     * {@link #userAgent()} before delegating to {@link #delegate()}.
     */
    @Override
    public final ManagedChannel create(ClientConfig config) {
        if (ssl().isPresent() && !config.ssl().isPresent() && config.target().isSecure()) {
            config = config.withSsl(ssl().get());
        }
        if (userAgent().isPresent() && !config.userAgent().isPresent()) {
            config = config.withUserAgent(userAgent().get());
        }
        return delegate().create(config);
    }

    public interface Builder {

        /**
         * Initializes the value for the {@link ClientChannelFactoryDefaulter#delegate() delegate} attribute.
         * <p>
         * If not set, this attribute will have a default value as returned by the initializer of
         * {@link ClientChannelFactoryDefaulter#delegate()}.
         */
        Builder delegate(ClientChannelFactory delegate);

        /**
         * Initializes the optional value {@link ClientChannelFactoryDefaulter#ssl() ssl} to ssl.
         */
        Builder ssl(SSLConfig ssl);

        /**
         * Initializes the optional value {@link ClientChannelFactoryDefaulter#userAgent() userAgent} to userAgent.
         */
        Builder userAgent(String userAgent);

        /**
         * Builds a new {@link ClientChannelFactoryDefaulter}.
         */
        ClientChannelFactoryDefaulter build();
    }
}
