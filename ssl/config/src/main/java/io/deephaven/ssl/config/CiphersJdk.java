/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import javax.net.ssl.SSLServerSocketFactory;

/**
 * Include the ciphers defined by the JDK.
 *
 * @see SSLServerSocketFactory#getDefaultCipherSuites()
 */
@Immutable
@SingletonStyle
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
