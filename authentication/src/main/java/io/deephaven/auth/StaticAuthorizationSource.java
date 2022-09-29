/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StaticAuthorizationSource implements AuthorizationSource {

    public static Builder allowBuilder() {
        return new Builder(AuthorizationResult.ALLOW);
    }

    public static Builder blockBuilder() {
        return new Builder(AuthorizationResult.BLOCK);
    }

    private final Set<Privilege> privileges;
    private final AuthorizationResult authorization;

    private StaticAuthorizationSource(
            final Set<Privilege> privileges,
            final AuthorizationResult authorization) {
        this.privileges = privileges;
        this.authorization = authorization;
    }

    @Override
    public AuthorizationResult hasPrivilege(Privilege privilege) {
        if (privileges.contains(privilege)) {
            return authorization;
        }
        return AuthorizationResult.UNDECIDED;
    }

    public static class Builder {
        private final Set<Privilege> privileges = new HashSet<>();
        private final AuthorizationResult authorization;

        public Builder(final AuthorizationResult authorization) {
            this.authorization = authorization;
        }

        public Builder with(Privilege... allowPrivileges) {
            privileges.addAll(Arrays.asList(allowPrivileges));
            return this;
        }

        @SafeVarargs
        public final Builder withAll(final Class<Privilege>... allowAllPrivileges) {
            for (final Class<Privilege> privilegeClass : allowAllPrivileges) {
                privileges.addAll(Arrays.asList(privilegeClass.getEnumConstants()));
            }
            return this;
        }

        public StaticAuthorizationSource build() {
            return new StaticAuthorizationSource(Collections.unmodifiableSet(privileges), authorization);
        }
    }
}
