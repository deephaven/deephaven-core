//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from the JDK.
 */
@Immutable
@SingletonStyle
@JsonDeserialize(as = ImmutableTrustJdk.class)
public abstract class TrustJdk extends TrustBase {

    public static TrustJdk of() {
        return ImmutableTrustJdk.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
