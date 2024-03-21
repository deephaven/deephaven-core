//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import picocli.CommandLine.Option;

import java.util.function.Consumer;

public class AuthenticationOptions {
    @Option(names = {"--mtls"}, description = "Use the connect mTLS")
    Boolean mtls;

    @Option(names = {"--psk"}, description = "The pre-shared key")
    String psk;

    @Option(names = {"--explicit"}, description = "The explicit authentication type and value")
    String explicit;

    public String toAuthenticationTypeAndValue() {
        if (mtls != null && mtls) {
            return "io.deephaven.authentication.mtls.MTlsAuthenticationHandler";
        }
        if (psk != null) {
            return "io.deephaven.authentication.psk.PskAuthenticationHandler " + psk;
        }
        if (explicit != null) {
            return explicit;
        }
        return null;
    }

    public void ifPresent(Consumer<String> consumer) {
        final String authenticationTypeAndValue = toAuthenticationTypeAndValue();
        if (authenticationTypeAndValue != null) {
            consumer.accept(authenticationTypeAndValue);
        }
    }
}
