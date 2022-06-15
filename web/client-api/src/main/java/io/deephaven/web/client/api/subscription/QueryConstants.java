/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.subscription;


/**
 * Constants for null values within the Deephaven engine From io.deephaven.util.QueryConstants
 */
public interface QueryConstants {
    char NULL_CHAR = Character.MAX_VALUE;
    byte NULL_BYTE = Byte.MIN_VALUE;
    short NULL_SHORT = Short.MIN_VALUE;
    int NULL_INT = Integer.MIN_VALUE;
    long NULL_LONG = Long.MIN_VALUE;
    float NULL_FLOAT = -Float.MAX_VALUE;
    double NULL_DOUBLE = -Double.MAX_VALUE;
    byte NULL_BOOLEAN_AS_BYTE = NULL_BYTE;
    byte TRUE_BOOLEAN_AS_BYTE = (byte) 1;
    byte FALSE_BOOLEAN_AS_BYTE = (byte) 0;
}
