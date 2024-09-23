//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.Result;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public interface ActionResolver {

    boolean supportsDoActionType(String type);

    void doAction(@Nullable final SessionState session, Action request, Consumer<Result> visitor);
}
