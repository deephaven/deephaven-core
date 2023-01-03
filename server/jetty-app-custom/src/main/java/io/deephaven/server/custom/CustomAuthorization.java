/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.custom;

import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.server.auth.CommunityAuthorizationProvider;

import javax.inject.Inject;

public final class CustomAuthorization extends CommunityAuthorizationProvider {
    @Inject
    public CustomAuthorization() {}

    @Override
    public InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring() {
        // Disable input table service
        return new InputTableServiceContextualAuthWiring.DenyAll();
    }
}
