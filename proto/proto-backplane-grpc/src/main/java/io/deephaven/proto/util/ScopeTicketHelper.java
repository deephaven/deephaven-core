package io.deephaven.proto.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ScopeTicketHelper {
    public static final char TICKET_PREFIX = 's';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "scope";

    /**
     * Convenience method to create the flight descriptor path for the query scope variable name.
     *
     * @param variableName the variable name
     * @return the path
     */
    public static List<String> nameToPath(String variableName) {
        return Arrays.asList(FLIGHT_DESCRIPTOR_ROUTE, variableName);
    }

    /**
     * Convenience method to create the flight ticket bytes for the query scope variable name.
     *
     * @param variableName the variable name
     * @return the ticket bytes
     */
    public static byte[] nameToBytes(String variableName) {
        return (TICKET_PREFIX + "/" + variableName).getBytes(StandardCharsets.UTF_8);
    }
}
