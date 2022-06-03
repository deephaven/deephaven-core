package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Include the ciphers defined by the system properties "https.cipherSuites".
 */
@Immutable
@SimpleStyle
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
