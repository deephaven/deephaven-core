/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The trust material from the system. Only applicable for Windows and Mac.
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableTrustSystem.class)
public abstract class TrustSystem implements Trust {

    public static TrustSystem of() {
        return ImmutableTrustSystem.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
