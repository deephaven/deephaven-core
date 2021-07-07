/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import org.apache.arrow.flight.impl.Flight;

public class TicketRouterHelper {
    /**
     * Create a human readable string to identify this ticket.
     *
     * @param descriptor the descriptor to parse
     * @return a string that is good for log/error messages
     */
    public static String getLogNameFor(final Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                "Flight descriptor is not a path");
        }

        final StringBuilder sb = new StringBuilder();
        for (int depth = 0; depth < descriptor.getPathCount(); ++depth) {
            sb.append("/");
            sb.append(descriptor.getPath(depth));
        }
        return sb.toString();
    }
}
