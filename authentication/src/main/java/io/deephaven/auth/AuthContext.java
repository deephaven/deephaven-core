/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.impl.LogOutputStringImpl;

public abstract class AuthContext implements AuthorizationSource, LogOutputAppendable {

    @Override
    public final String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    /**
     * A trivial auth context that allows a user to do everything the APIs allow.
     */
    public static class SuperUser extends AuthContext {
        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("SuperUser");
        }

        @Override
        public AuthorizationResult hasPrivilege(Privilege privilege) {
            return AuthorizationResult.ALLOW;
        }
    }

    public static class Anonymous extends AuthContext {
        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("Anonymous");
        }

        @Override
        public AuthorizationResult hasPrivilege(Privilege privilege) {
            return AuthorizationResult.ALLOW;
        }
    }

    public static class ConfiguredAuthContext extends AuthContext {
        private final String username;
        private final AuthorizationSource authorizationSource;

        public ConfiguredAuthContext(final String username, final AuthorizationSource authorizationSource) {
            this.authorizationSource = authorizationSource;
            this.username = username;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("AuthContext{username=").append(username).append('}');
        }

        @Override
        public AuthorizationResult hasPrivilege(final Privilege privilege) {
            return authorizationSource.hasPrivilege(privilege);
        }
    }

    public static class BlockAllAuthContext extends AuthContext {
        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("NoAuthorization");
        }

        @Override
        public AuthorizationResult hasPrivilege(final Privilege privilege) {
            return AuthorizationResult.BLOCK;
        }
    }
}
