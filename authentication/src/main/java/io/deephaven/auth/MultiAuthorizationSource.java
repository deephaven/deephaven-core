/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

/**
 * An AuthorizationSource that composes multiple AuthorizationSources together.
 * <p>
 * If an AuthorizationSource returns UNDECIDED, the next AuthorizationSource is consulted. If all AuthorizationSources
 * return UNDECIDED, the result is UNDECIDED.
 */
public class MultiAuthorizationSource implements AuthorizationSource {
    private final AuthorizationSource[] authorizationSources;

    public MultiAuthorizationSource(final AuthorizationSource... authorizationSources) {
        this.authorizationSources = authorizationSources;
    }

    public AuthorizationResult hasPrivilege(final Privilege privilege) {
        for (final AuthorizationSource authorizationSource : authorizationSources) {
            AuthorizationResult authorizationResponse = authorizationSource.hasPrivilege(privilege);
            if (authorizationResponse != AuthorizationResult.UNDECIDED) {
                return authorizationResponse;
            }
        }
        return AuthorizationResult.UNDECIDED;
    }
}
