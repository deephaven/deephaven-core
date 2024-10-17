//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.Result;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public final class ActionRouter {

    private static final Logger log = LoggerFactory.getLogger(ActionRouter.class);

    private final Set<ActionResolver> resolvers;

    @Inject
    public ActionRouter(Set<ActionResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
    }

    public void listActions(@Nullable final SessionState session, Consumer<ActionType> visitor) {
        for (ActionResolver resolver : resolvers) {
            resolver.forAllFlightActionType(session, visitor);
        }
    }

    public void doAction(@Nullable final SessionState session, Action request, Consumer<Result> visitor) {
        final String type = request.getType();
        ActionResolver actionResolver = null;
        for (ActionResolver resolver : resolvers) {
            if (!resolver.supportsDoActionType(type)) {
                continue;
            }
            if (actionResolver != null) {
                throw Exceptions.statusRuntimeException(Code.INTERNAL,
                        String.format("Found multiple doAction resolvers for action type '%s'", type));
            }
            actionResolver = resolver;
        }
        if (actionResolver == null) {
            // Similar to the default unimplemented message from
            // org.apache.arrow.flight.impl.FlightServiceGrpc.AsyncService.doAction
            throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                    String.format("No action resolver found for action type '%s'", type));
        }
        actionResolver.doAction(session, request, visitor);
    }
}
