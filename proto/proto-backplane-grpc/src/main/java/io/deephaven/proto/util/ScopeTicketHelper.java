//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto.util;

import org.apache.commons.codec.binary.Hex;

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
     * Convenience method to decode the scope ticket bytes into a human-readable description.
     *
     * @param ticket the ticket bytes
     * @return the human-readable description
     */
    public static String toReadableString(final byte[] ticket) {
        if (ticket.length < 3 || ticket[0] != TICKET_PREFIX || ticket[1] != '/') {
            throw new IllegalArgumentException(String.format(
                    "QueryScope ticket does not conform to expected format; found '0x%s", Hex.encodeHexString(ticket)));
        }

        final String ticketAsString;
        final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        try {
            ticketAsString = decoder.decode(ByteBuffer.wrap(ticket)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(String.format(
                    "Failed to decode query scope ticket; found '0x%s'", Hex.encodeHexString(ticket)), e);
        }
        final String fieldName = ticketAsString.substring(2);

        return String.format("%s/%s", FLIGHT_DESCRIPTOR_ROUTE, fieldName);
    }
}
