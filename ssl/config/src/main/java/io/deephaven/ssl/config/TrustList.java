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
 * A trust materials list.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableTrustList.class)
public abstract class TrustList extends TrustBase {

    public static TrustList of(Trust... elements) {
        return ImmutableTrustList.builder().addValues(elements).build();
    }

    public static TrustList of(List<? extends Trust> elements) {
        return ImmutableTrustList.builder().addAllValues(elements).build();
    }

    public abstract List<Trust> values();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final boolean contains(Trust trust) {
        return values().stream().anyMatch(t -> t.contains(trust));
    }

    @Override
    public final Trust or(Trust other) {
        if (contains(other)) {
            return this;
        }
        if (other.contains(this)) {
            return other;
        }
        return ImmutableTrustList.builder().addAllValues(values()).addValues(other).build();
    }

    @Check
    final void checkNonEmpty() {
        if (values().isEmpty()) {
            throw new IllegalArgumentException("values() must be non-empty");
        }
    }
}
