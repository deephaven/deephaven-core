//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from the system properties "javax.net.ssl.trustStore", "javax.net.ssl.trustStorePassword",
 * "javax.net.ssl.trustStoreType", and "javax.net.ssl.trustStoreProvider".
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableTrustProperties.class)
public abstract class TrustProperties extends TrustBase {

    public static TrustProperties of() {
        return ImmutableTrustProperties.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
