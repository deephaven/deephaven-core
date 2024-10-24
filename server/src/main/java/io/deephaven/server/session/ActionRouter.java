//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Result;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public final class ActionRouter {

    private final Set<ActionResolver> resolvers;

    @Inject
    public ActionRouter(Set<ActionResolver> resolvers) {
        this.resolvers = Objects.requireNonNull(resolvers);
    }

    /**
     * Invokes {@code visitor} for all of the resolvers. Used as the basis for implementing FlightService ListActions.
     *
     * @param session the session
     * @param visitor the visitor
     */
    public void listActions(@Nullable final SessionState session, final Consumer<ActionType> visitor) {
        for (ActionResolver resolver : resolvers) {
            resolver.listActions(session, visitor);
        }
    }

    /**
     * Routes {@code action} to the appropriate {@link ActionResolver}. Used as the basis for implementing FlightService
     * DoAction.
     *
     * @param session the session
     * @param action the action
     * @param observer the results observer
     * @throws io.grpc.StatusRuntimeException if zero or more than one resolver is found
     */
    public void doAction(@Nullable final SessionState session, final Action action,
            final ActionResolver.ActionObserver observer) {
        getResolver(action.getType()).doAction(session, action, observer);
    }

    private ActionResolver getResolver(final String type) {
        ActionResolver actionResolver = null;
        // This is the most "naive" resolution logic; it scales linearly with the number of resolvers, but it is the
        // most general and may be the best we can do for certain types of action protocols built on top of Flight. If
        // we find the number of action resolvers scaling up, we could devise a more efficient strategy in some cases
        // either based on a prefix model and/or a fixed set model (which could be communicated either through new
        // method(s) on ActionResolver, or through subclasses).
        //
        // Regardless, even with a moderate amount of action resolvers, the linear nature of this should not be a
        // bottleneck.
        for (ActionResolver resolver : resolvers) {
            if (!resolver.handlesActionType(type)) {
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
        return actionResolver;
    }
}
