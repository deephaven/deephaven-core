package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import javax.net.ssl.SSLSocket;

/**
 * Include the protocols defined by the JDK.
 *
 * @see SSLSocket#getSupportedProtocols()
 */
@Immutable
@SimpleStyle
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
