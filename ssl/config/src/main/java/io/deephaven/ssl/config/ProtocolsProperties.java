//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * Include the protocols defined by the system properties "https.protocols".
 */
@Immutable
@SingletonStyle
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
