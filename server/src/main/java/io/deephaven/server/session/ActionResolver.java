//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.Result;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public interface ActionResolver {

    // this is a _routing_ question after a client has already sent a request
    boolean supportsDoActionType(String type);

    // this is a _capabilities_ question a client can inquire about
    // note this is the Flight object and not the gRPC object (like TicketResolver)
    // todo: is listActions a better name?
    void forAllFlightActionType(@Nullable SessionState session, Consumer<ActionType> visitor);

    void doAction(@Nullable final SessionState session, Action request, Consumer<Result> visitor);
}
