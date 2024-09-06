//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.custom;

import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.server.auth.AllowAllAuthorizationProvider;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.auth.CommunityAuthorizationProvider;

import javax.inject.Inject;

/**
 * Simple authorization that "allows all" except "denys all" for the {@link #getInputTableServiceContextualAuthWiring()
 * input table service}.
 */
public final class CustomAuthorization extends AllowAllAuthorizationProvider {

    @Inject
    public CustomAuthorization() {}

    @Override
    public InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring() {
        // Disable input table service
        return new InputTableServiceContextualAuthWiring.DenyAll();
    }
}
