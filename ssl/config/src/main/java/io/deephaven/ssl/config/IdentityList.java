//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * An identity materials list.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableIdentityList.class)
public abstract class IdentityList implements Identity {

    public static IdentityList of(Identity... identities) {
        return ImmutableIdentityList.builder().addValues(identities).build();
    }

    public static IdentityList of(List<? extends Identity> identities) {
        return ImmutableIdentityList.builder().addAllValues(identities).build();
    }

    public abstract List<Identity> values();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}
