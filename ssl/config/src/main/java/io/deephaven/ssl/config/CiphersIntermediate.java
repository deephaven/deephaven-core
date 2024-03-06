//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.List;

/**
 * Includes intermediate ciphers for TLSv1.3 and TLSv1.2.
 *
 * @see <a href=
 *      "https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29">Intermediate
 *      compatibility</a>
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableCiphersIntermediate.class)
public abstract class CiphersIntermediate implements Ciphers {

    public static CiphersIntermediate of() {
        return ImmutableCiphersIntermediate.of();
    }

    public final List<String> ciphers() {
        // Note: not including DHE ciphers here, as the default JDK does not easily allow customizing DH params.
        // See jdk.tls.server.defaultDHEParameters
        // https://docs.oracle.com/en/java/javase/11/security/java-secure-socket-extension-jsse-reference-guide.html#GUID-A41282C3-19A3-400A-A40F-86F4DA22ABA9
        return Arrays.asList(
                // TLS 1.3
                "TLS_AES_256_GCM_SHA384",
                "TLS_AES_128_GCM_SHA256",
                "TLS_CHACHA20_POLY1305_SHA256",

                // TLS 1.2
                "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
