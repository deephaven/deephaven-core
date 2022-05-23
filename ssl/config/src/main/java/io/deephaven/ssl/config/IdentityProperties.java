package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * Include the identity material defined by the system properties "javax.net.ssl.keyStore",
 * "javax.net.ssl.keyStorePassword", "javax.net.ssl.keyStoreType", and "javax.net.ssl.keyStoreProvider".
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableIdentityProperties.class)
public abstract class IdentityProperties implements Identity {

    public static IdentityProperties of() {
        return ImmutableIdentityProperties.of();
    }

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}
