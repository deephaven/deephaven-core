//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.ClientConfig.Builder;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import picocli.CommandLine.Option;

import java.util.Map;

public class ConnectOptions {

    public static final String DEFAULT_HOST = "localhost";

    public static final DeephavenTarget DEFAULT_TARGET = DeephavenTarget.builder()
            .host(DEFAULT_HOST)
            .isSecure(false)
            .build();

    public static final String DEFAULT_TARGET_VALUE = DeephavenUri.PLAINTEXT_SCHEME + "://" + DEFAULT_HOST;

    public static ConnectOptions options(ConnectOptions options) {
        if (options == null) {
            options = new ConnectOptions();
            // https://github.com/remkop/picocli/issues/844
            options.target = DEFAULT_TARGET;
        }
        return options;
    }

    @Option(names = {"-t", "--target"}, description = "The target, defaults to ${DEFAULT-VALUE}",
            defaultValue = DEFAULT_TARGET_VALUE, converter = DeephavenTargetConverter.class)
    DeephavenTarget target;

    @Option(names = {"-u", "--user-agent"}, description = "The user-agent.")
    String userAgent;

    @Option(names = {"--override-authority"}, description = "The override authority.")
    String overrideAuthority;

    @Option(names = {"--max-inbound-message-size"}, description = "The maximum inbound message size, " +
            "defaults to 100MB")
    Integer maxInboundMessageSize;

    @Option(names = {"--ssl"}, description = "The optional ssl config file.", converter = SSLConverter.class)
    SSLConfig ssl;

    @Option(names = {"--header"})
    Map<String, String> headers;

    public ClientConfig config() {
        final Builder builder = ClientConfig.builder().target(target);
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }
        if (overrideAuthority != null) {
            builder.overrideAuthority(overrideAuthority);
        }
        if (maxInboundMessageSize != null) {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        if (ssl != null) {
            builder.ssl(ssl);
        }
        if (headers != null) {
            builder.putAllExtraHeaders(headers);
        }
        return builder.build();
    }
}
