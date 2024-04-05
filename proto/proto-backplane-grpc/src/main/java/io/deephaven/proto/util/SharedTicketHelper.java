//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class SharedTicketHelper {
    public static final char TICKET_PREFIX = 'h';
    public static final String FLIGHT_DESCRIPTOR_ROUTE = "shared";

    /**
     * Convenience method to create the flight descriptor path for the given shared variable identifier.
     *
     * @param variableId the variable identifier
     * @return the path
     */
    public static List<String> nameToPath(String variableId) {
        return Arrays.asList(FLIGHT_DESCRIPTOR_ROUTE, variableId);
    }

    /**
     * Convenience method to create the flight ticket bytes for the given shared variable identifier.
     *
     * @param variableId the variable identifier
     * @return the ticket bytes
     */
    public static byte[] nameToBytes(String variableId) {
        return (TICKET_PREFIX + "/" + variableId).getBytes(StandardCharsets.UTF_8);
    }
}
