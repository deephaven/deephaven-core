/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.List;

/**
 * Includes modern ciphers for TLSv1.3 and TLSv1.2.
 *
 * @see <a href="https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility">Modern compatibility</a>
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableCiphersModern.class)
public abstract class CiphersModern implements Ciphers {

    public static CiphersModern of() {
        return ImmutableCiphersModern.of();
    }

    public final List<String> ciphers() {
        return Arrays.asList(
                // TLS 1.3
                "TLS_AES_256_GCM_SHA384",
                "TLS_AES_128_GCM_SHA256",
                "TLS_CHACHA20_POLY1305_SHA256");
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
