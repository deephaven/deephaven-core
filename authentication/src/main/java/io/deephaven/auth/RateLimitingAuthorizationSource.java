/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import com.google.common.util.concurrent.RateLimiter;

import java.util.Optional;

/**
 * An authorization source that wraps a function that provides the rate limiter for a given privilege.
 *
 * @apiNote The function should cache the rate limiters it creates, as this class will call it for every request.
 */
@SuppressWarnings("UnstableApiUsage")
public class RateLimitingAuthorizationSource implements AuthorizationSource {
    public interface RateLimiterProvider {
        Optional<RateLimiter> getRateLimiter(Privilege name);
    }

    private final RateLimiterProvider rateLimiterProvider;

    public RateLimitingAuthorizationSource(final RateLimiterProvider rateLimiterProvider) {
        this.rateLimiterProvider = rateLimiterProvider;
    }

    @Override
    public AuthorizationResult hasPrivilege(Privilege privilege) {
        final Optional<RateLimiter> rateLimiter;
        synchronized (rateLimiterProvider) {
            rateLimiter = rateLimiterProvider.getRateLimiter(privilege);
        }
        if (!rateLimiter.map(RateLimiter::tryAcquire).orElse(true)) {
            return AuthorizationResult.BLOCK;
        }
        return AuthorizationResult.UNDECIDED;
    }
}
