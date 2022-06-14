/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.ClientConfig.Builder;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.grpc.ManagedChannel;
import picocli.CommandLine.Option;

public class ConnectOptions {

    public static final String DEFAULT_HOST = "localhost";

    public static final DeephavenTarget DEFAULT_TARGET = DeephavenTarget.builder()
            .host(DEFAULT_HOST)
            .isSecure(false)
            .build();

    public static final String DEFAULT_TARGET_VALUE = DeephavenUri.PLAINTEXT_SCHEME + "://" + DEFAULT_HOST;

    public static ManagedChannel open(ConnectOptions options) {
        if (options == null) {
            options = new ConnectOptions();
            // https://github.com/remkop/picocli/issues/844
            options.target = DEFAULT_TARGET;
        }
        return options.open();
    }

    @Option(names = {"-t", "--target"}, description = "The target, defaults to ${DEFAULT-VALUE}",
            defaultValue = DEFAULT_TARGET_VALUE, converter = DeephavenTargetConverter.class)
    DeephavenTarget target;

    @Option(names = {"-u", "--user-agent"}, description = "The user-agent.")
    String userAgent;

    @Option(names = {"--max-inbound-message-size"}, description = "The maximum inbound message size, " +
            "defaults to 100MB")
    Integer maxInboundMessageSize;

    @Option(names = {"--ssl"}, description = "The optional ssl config file.", converter = SSLConverter.class)
    SSLConfig ssl;

    private ClientConfig config() {
        final Builder builder = ClientConfig.builder().target(target);
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }
        if (maxInboundMessageSize != null) {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        if (ssl != null) {
            builder.ssl(ssl);
        }
        return builder.build();
    }

    public ManagedChannel open() {
        return ChannelHelper.channel(config());
    }
}
