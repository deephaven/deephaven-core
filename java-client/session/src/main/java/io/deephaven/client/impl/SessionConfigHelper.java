//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import java.time.Duration;

import static java.lang.Boolean.parseBoolean;

final class SessionConfigHelper {
    static boolean delegateToBatch() {
        final String property = System.getProperty(SessionImplConfig.DEEPHAVEN_SESSION_BATCH);
        return property == null || parseBoolean(property);
    }

    static boolean mixinStacktrace() {
        return Boolean.getBoolean(SessionImplConfig.DEEPHAVEN_SESSION_BATCH_STACKTRACES);
    }

    static Duration executeTimeout() {
        return Duration.parse(System.getProperty(SessionImplConfig.DEEPHAVEN_SESSION_EXECUTE_TIMEOUT, "PT1m"));
    }

    static Duration closeTimeout() {
        return Duration.parse(System.getProperty(SessionImplConfig.DEEPHAVEN_SESSION_CLOSE_TIMEOUT, "PT5s"));
    }
}
