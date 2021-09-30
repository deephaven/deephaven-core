package io.deephaven.client.examples;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine.Option;

public class ConnectOptions {

    private static final String TARGET_DEFAULT = "localhost:10000";

    // https://github.com/remkop/picocli/issues/844
    public static ConnectOptions defaults() {
        final ConnectOptions options = new ConnectOptions();
        options.target = TARGET_DEFAULT;
        return options;
    }

    public static ManagedChannel open(ConnectOptions options) {
        if (options == null) {
            options = defaults();
        }
        return options.open();
    }

    @Option(names = {"-t", "--target"}, description = "The host target, default: ${DEFAULT-VALUE}",
            defaultValue = TARGET_DEFAULT)
    String target;

    @Option(names = {"-p", "--plaintext"}, description = "The plaintext flag.")
    Boolean plaintext;

    @Option(names = {"-u", "--user-agent"}, description = "The user-agent.")
    String userAgent;

    public ManagedChannel open() {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
        if (plaintext == null) {
            if (target.startsWith("localhost:")) {
                channelBuilder.usePlaintext();
            } else {
                channelBuilder.useTransportSecurity();
            }
        } else {
            if (plaintext) {
                channelBuilder.usePlaintext();
            } else {
                channelBuilder.useTransportSecurity();
            }
        }
        if (userAgent != null) {
            channelBuilder.userAgent(userAgent);
        }
        return channelBuilder.build();
    }
}
