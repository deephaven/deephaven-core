/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from the JDK.
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableTrustJdk.class)
public abstract class TrustJdk implements Trust {

    public static TrustJdk of() {
        return ImmutableTrustJdk.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
