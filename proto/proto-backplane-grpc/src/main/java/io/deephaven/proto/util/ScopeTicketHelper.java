//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
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

    /**
     * Convenience method to decode the application ticket bytes into a human-readable description.
     *
     * @param ticket the ticket bytes
     * @return the human-readable description
     */
    public static String toReadableString(final byte[] ticket) {
        final String ticketAsString;
        final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        try {
            ticketAsString = decoder.decode(ByteBuffer.wrap(ticket)).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException("Failed to decode query scope ticket: " + e.getMessage(), e);
        }

        final int endOfRoute = ticketAsString.indexOf('/');
        if (endOfRoute == -1) {
            throw new RuntimeException("QueryScope ticket does not conform to expected format");
        }
        final String fieldName = ticketAsString.substring(endOfRoute + 1);

        return String.format("%s/%s", FLIGHT_DESCRIPTOR_ROUTE, fieldName);
    }
}
