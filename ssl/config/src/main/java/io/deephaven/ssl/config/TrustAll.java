/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The unsafe trust that will trust all certificates without validation.
 */
@Immutable
@SimpleStyle
@JsonDeserialize(as = ImmutableTrustAll.class)
public abstract class TrustAll implements Trust {

    public static TrustAll of() {
        return ImmutableTrustAll.of();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
