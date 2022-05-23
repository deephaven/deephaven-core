package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Include the protocols defined by the system properties "https.protocols".
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableProtocolsProperties.class)
public abstract class ProtocolsProperties implements Protocols {

    public static ProtocolsProperties of() {
        return ImmutableProtocolsProperties.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
