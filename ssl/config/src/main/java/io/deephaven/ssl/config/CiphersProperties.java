//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * Include the ciphers defined by the system properties "https.cipherSuites".
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableCiphersProperties.class)
public abstract class CiphersProperties implements Ciphers {

    public static CiphersProperties of() {
        return ImmutableCiphersProperties.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
