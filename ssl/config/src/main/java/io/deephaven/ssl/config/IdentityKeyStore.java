/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ssl.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * The identity material configuration from a key store.
 */
@Immutable
@BuildableStyle
@JsonDeserialize(as = ImmutableIdentityKeyStore.class)
public abstract class IdentityKeyStore implements Identity {

    public static IdentityKeyStore of(String path, String password) {
        return ImmutableIdentityKeyStore.builder().path(path).password(password).build();
    }

    public static IdentityKeyStore of(String path, String password, String keystoreType) {
        return ImmutableIdentityKeyStore.builder().path(path).password(password).keystoreType(keystoreType).build();
    }

    /**
     * The keystore path.
     */
    public abstract String path();

    /**
     * The keystore password.
     */
    public abstract String password();

    /**
     * The optional keystore type.
     */
    public abstract Optional<String> keystoreType();

    @Override
    public final <V extends Visitor<T>, T> T walk(V visitor) {
        return visitor.visit(this);
    }
}
