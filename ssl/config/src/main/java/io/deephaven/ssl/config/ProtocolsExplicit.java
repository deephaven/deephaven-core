/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Include the protocols explicitly defined.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableProtocolsExplicit.class)
public abstract class ProtocolsExplicit implements Protocols {

    public static ProtocolsExplicit of(String... values) {
        return ImmutableProtocolsExplicit.builder().addValues(values).build();
    }

    public static ProtocolsExplicit of(List<String> values) {
        return ImmutableProtocolsExplicit.builder().addAllValues(values).build();
    }

    public abstract List<String> values();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkNonEmpty() {
        if (values().isEmpty()) {
            throw new IllegalArgumentException("values() must be non-empty");
        }
    }
}
