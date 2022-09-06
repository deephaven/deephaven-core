/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.impl.LogOutputStringImpl;

public abstract class AuthContext implements LogOutputAppendable {

    @Override
    public abstract LogOutput append(LogOutput logOutput);

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
    }

    public static class Anonymous extends AuthContext {
        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("Anonymous");
        }
    }
}
