/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from a trust store.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustStore.class)
public abstract class TrustStore extends TrustBase {
    public static TrustStore of(String path, String password) {
        return ImmutableTrustStore.builder().path(path).password(password).build();
    }

    /**
     * The trust store path.
     */
    public abstract String path();

    /**
     * The trust storce password.
     */
    public abstract String password();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
