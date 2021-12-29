package io.deephaven.client.examples;

import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine.Option;

public class ConnectOptions {

    public static final String DEFAULT_HOST = "localhost";

    public static final int DEFAULT_PORT = 10000;

    public static final DeephavenTarget DEFAULT_TARGET = DeephavenTarget.builder()
            .host(DEFAULT_HOST)
            .port(DEFAULT_PORT)
            .isSecure(false)
            .build();

    public static final String DEFAULT_TARGET_VALUE =
            DeephavenUri.PLAINTEXT_SCHEME + "://" + DEFAULT_HOST + ":" + DEFAULT_PORT;

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

    public ManagedChannel open() {
        final ManagedChannelBuilder<?> builder =
                ManagedChannelBuilder.forAddress(target.host(), target.port().orElse(DEFAULT_PORT));
        if (target.isSecure()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }
        return builder.build();
    }
}
