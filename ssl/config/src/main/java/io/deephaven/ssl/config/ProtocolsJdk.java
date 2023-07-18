/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

import javax.net.ssl.SSLSocket;

/**
 * Include the protocols defined by the JDK.
 *
 * @see SSLSocket#getSupportedProtocols()
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableProtocolsJdk.class)
public abstract class ProtocolsJdk implements Protocols {

    public static ProtocolsJdk of() {
        return ImmutableProtocolsJdk.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
