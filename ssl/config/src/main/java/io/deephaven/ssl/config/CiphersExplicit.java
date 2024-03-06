//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Include the ciphers explicitly defined.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableCiphersExplicit.class)
public abstract class CiphersExplicit implements Ciphers {

    public static CiphersExplicit of(String... values) {
        return ImmutableCiphersExplicit.builder().addValues(values).build();
    }

    public static CiphersExplicit of(List<String> values) {
        return ImmutableCiphersExplicit.builder().addAllValues(values).build();
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
