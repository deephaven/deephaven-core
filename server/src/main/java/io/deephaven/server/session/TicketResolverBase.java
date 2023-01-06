/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.server.auth.AuthorizationProvider;

public abstract class TicketResolverBase implements TicketResolver {

    @FunctionalInterface
    public interface AuthTransformation {
        /**
         * Implementations must type check the provided source as any type of object can be stored in an export.
         * <p>
         *
         * @apiNote Types typically encountered are {@link Table} and {@link PartitionedTable}. Perform an identity
         *          mapping for any types that you do not wish to transform. This method should not error.
         *          Implementations may wish to query {@link ExecutionContext#getAuthContext()} to apply user-specific
         *          transformations to requested resources.
         *
         * @param source the object to transform (such as by applying ACLs)
         * @return an object that has been sanitized to be used by the current user
         */
        <T> T transform(T source);
    }

    public enum IdentityTransformation implements AuthTransformation {
        INSTANCE;

        @Override
        public <T> T transform(T source) {
            return source;
        }
    }

    public static AuthTransformation identityTransformation() {
        return IdentityTransformation.INSTANCE;
    }

    protected final AuthTransformation authTransformation;
    private final byte ticketPrefix;
    private final String flightDescriptorRoute;

    public TicketResolverBase(
            final AuthorizationProvider authProvider,
            final byte ticketPrefix, final String flightDescriptorRoute) {
        this.authTransformation = authProvider.getTicketTransformation();
        this.ticketPrefix = ticketPrefix;
        this.flightDescriptorRoute = flightDescriptorRoute;
    }

    @Override
    public byte ticketRoute() {
        return ticketPrefix;
    }

    @Override
    public String flightDescriptorRoute() {
        return flightDescriptorRoute;
    }
}
