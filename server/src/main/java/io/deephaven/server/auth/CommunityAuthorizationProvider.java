//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.auth;

import javax.inject.Inject;

/**
 * The out-of-the-box authorization provider for the Deephaven community server.
 *
 * <p>
 * This is currently implemented as an "allow all" authorization provider. This is subject to change in the future with
 * the addition of community authorization configuration parameters.
 */
public class CommunityAuthorizationProvider extends AllowAllAuthorizationProvider {
    @Inject
    public CommunityAuthorizationProvider() {}
}
