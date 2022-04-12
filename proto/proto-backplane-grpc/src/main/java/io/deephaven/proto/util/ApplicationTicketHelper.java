package io.deephaven.proto.util;

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
}
