/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.auth;

import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface AuthContext {

    /**
     * Get a role name for this auth context.
     *
     * @return The role name for this auth context
     */
    @NotNull
    String getAuthRoleName();

    /**
     * Get an implementation-specific identifier that may be used as a key, for audit logging, etc.
     *
     * @return The authentication identifier for this context
     */
    @NotNull
    String getAuthId();

    /**
     * Get a log representation for this auth context.
     *
     * @return The log representation
     */
    @FinalDefault
    @NotNull
    default String getLogRepresentation() {
        return getAuthRoleName() + ':' + getAuthId();
    }

    /**
     * A trivial auth context that allows a user to do everything the APIs allow.
     */
    class SuperUser implements AuthContext {

        @NotNull
        public final String getAuthRoleName() {
            return "SuperUser";
        }

        @NotNull
        public final String getAuthId() {
            return "default";
        }
    }
}
