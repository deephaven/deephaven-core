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

public class ApplicationTicketHelper {

    public static final char TICKET_PREFIX = 'a';

    public static final String FLIGHT_DESCRIPTOR_ROUTE = "app";

    public static final String FIELD_PATH_SEGMENT = "field";

    /**
     * Convenience method to create the flight descriptor path for the application field.
     *
     * @param applicationId the application ID
     * @param fieldName the field name
     * @return the path
     */
    public static List<String> applicationFieldToPath(String applicationId, String fieldName) {
        return Arrays.asList(FLIGHT_DESCRIPTOR_ROUTE, applicationId, FIELD_PATH_SEGMENT, fieldName);
    }

    /**
     * Convenience method to create the flight ticket bytes for the application field.
     *
     * @param applicationId the application ID
     * @param fieldName the field name
     * @return the ticket bytes
     */
    public static byte[] applicationFieldToBytes(String applicationId, String fieldName) {
        return (TICKET_PREFIX + "/" + applicationId + "/f/" + fieldName).getBytes(StandardCharsets.UTF_8);
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
            throw new IllegalArgumentException(String.format(
                    "Failed to decode application field ticket; found '0x%s'", Hex.encodeHexString(ticket)), e);
        }

        final int endOfRoute = ticketAsString.indexOf('/');
        final int endOfAppId = ticketAsString.indexOf('/', endOfRoute + 1);
        final int endOfFieldSegment = ticketAsString.indexOf('/', endOfAppId + 1);
        if (endOfFieldSegment == -1) {
            throw new IllegalArgumentException(String.format(
                    "Application field ticket does not conform to expected format; found '0x%s'",
                    Hex.encodeHexString(ticket)));
        }
        final String appId = ticketAsString.substring(endOfRoute + 1, endOfAppId);
        final String fieldName = ticketAsString.substring(endOfFieldSegment + 1);

        return String.format("%s/%s/%s/%s", FLIGHT_DESCRIPTOR_ROUTE, appId, FIELD_PATH_SEGMENT, fieldName);
    }
}
