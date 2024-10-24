//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import org.apache.arrow.flight.impl.Flight;

import java.util.Objects;

/**
 * A specialization of {@link PathResolver} whose path {@link Flight.FlightDescriptor} resolution is based on the first
 * path in the list.
 */
public abstract class PathResolverPrefixedBase implements PathResolver {

    private final String flightDescriptorRoute;

    public PathResolverPrefixedBase(String flightDescriptorRoute) {
        this.flightDescriptorRoute = Objects.requireNonNull(flightDescriptorRoute);
    }

    /**
     * The first path entry on a route indicates which resolver to use. The remaining path elements are used to resolve
     * the descriptor.
     *
     * @return the string that will route from flight descriptor to this resolver
     */
    public final String flightDescriptorRoute() {
        return flightDescriptorRoute;
    }

    /**
     * Returns {@code true} if the first path in {@code descriptor} is equal to {@link #flightDescriptorRoute()}.
     *
     * @param descriptor the descriptor
     * @return {@code true} if this resolver handles the descriptor path
     */
    @Override
    public final boolean handlesPath(Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw new IllegalStateException("descriptor is not a path");
        }
        if (descriptor.getPathCount() == 0) {
            return false;
        }
        return flightDescriptorRoute.equals(descriptor.getPath(0));
    }
}
