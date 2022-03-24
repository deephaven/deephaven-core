package io.deephaven.client.examples;

import io.deephaven.client.impl.ChannelHelper;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
    int maxInboundMessageSize;

    public ManagedChannel open() {
        final ManagedChannelBuilder<?> builder = ChannelHelper.channelBuilder(target);
        if (userAgent != null) {
            builder.userAgent(userAgent);
        }
        if (maxInboundMessageSize == 0) {
            builder.maxInboundMessageSize(100 * 1024 * 1024); // 100MB default message size
        } else {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        return builder.build();
    }
}
