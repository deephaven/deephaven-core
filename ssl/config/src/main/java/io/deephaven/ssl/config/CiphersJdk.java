package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import javax.net.ssl.SSLServerSocketFactory;

/**
 * Include the ciphers defined by the JDK.
 *
 * @see SSLServerSocketFactory#getDefaultCipherSuites()
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableCiphersJdk.class)
public abstract class CiphersJdk implements Ciphers {

    public static CiphersJdk of() {
        return ImmutableCiphersJdk.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
