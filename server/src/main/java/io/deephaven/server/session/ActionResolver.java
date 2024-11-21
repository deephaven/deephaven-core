//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Result;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public interface ActionResolver {

    /**
     * Invokes the {@code visitor} for the specific action types that this implementation supports for the given
     * {@code session}; it should be the case that all consumed action types return {@code true} against
     * {@link #handlesActionType(String)}. Unlike {@link #handlesActionType(String)}, the implementations should
     * <b>not</b> invoke the visitor for action types in their domain that they do not implement.
     *
     * <p>
     * This is called in the context of {@link ActionRouter#listActions(SessionState, Consumer)} to allow flight
     * consumers to understand the capabilities of this flight service.
     *
     * @param session the session
     * @param visitor the visitor
     */
    void listActions(@Nullable SessionState session, Consumer<ActionType> visitor);

    /**
     * Returns {@code true} if this resolver is responsible for handling the action {@code type}. Implementations should
     * prefer to return {@code true} if they know the action type is in their domain even if they don't implement it;
     * this allows them to provide a more specific error message for unimplemented action types.
     *
     * <p>
     * This is used in support of routing in {@link ActionRouter#doAction(SessionState, Action, StreamObserver)} calls.
     *
     * @param type the action type
     * @return {@code true} if this resolver handles the action type
     */
    boolean handlesActionType(String type);

    /**
     * Executes the given {@code action}. Should only be called if {@link #handlesActionType(String)} is {@code true}
     * for the given {@code action}.
     *
     * <p>
     * This is called in the context of {@link ActionRouter#doAction(SessionState, Action, StreamObserver)} to allow
     * flight consumers to execute an action against this flight service.
     *
     * @param session the session
     * @param action the action
     * @param observer the observer
     */
    void doAction(@Nullable final SessionState session, final Action action, final StreamObserver<Result> observer);
}
