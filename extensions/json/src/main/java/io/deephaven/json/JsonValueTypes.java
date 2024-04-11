//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import java.util.EnumSet;

/**
 * The JSON value types.
 */
public enum JsonValueTypes {
    OBJECT, ARRAY, STRING, INT, DECIMAL, BOOL, NULL;

    /**
     * The set of all {@link JsonValueTypes}.
     */
    public static final EnumSet<JsonValueTypes> ALL = EnumSet.allOf(JsonValueTypes.class);

    /**
     * The set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> INT_OR_NULL = EnumSet.of(INT, NULL);

    /**
     * The set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> INT_LIKE = EnumSet.of(INT, STRING, NULL);

    /**
     * The set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}.
     */
    public static final EnumSet<JsonValueTypes> NUMBER = EnumSet.of(INT, DECIMAL);

    /**
     * The set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> NUMBER_OR_NULL = EnumSet.of(INT, DECIMAL, NULL);

    /**
     * The set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}, {@link JsonValueTypes#STRING},
     * {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> NUMBER_LIKE = EnumSet.of(INT, DECIMAL, STRING, NULL);

    /**
     * The set of {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> STRING_OR_NULL = EnumSet.of(STRING, NULL);

    /**
     * The set of {@link JsonValueTypes#STRING}, {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL},
     * {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> STRING_LIKE = EnumSet.of(STRING, INT, DECIMAL, BOOL, NULL);

    /**
     * The set of {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> BOOL_OR_NULL = EnumSet.of(BOOL, NULL);

    /**
     * The set of {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> BOOL_LIKE = EnumSet.of(STRING, BOOL, NULL);

    /**
     * The set of {@link JsonValueTypes#OBJECT}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> OBJECT_OR_NULL = EnumSet.of(OBJECT, NULL);

    /**
     * The set of {@link JsonValueTypes#ARRAY}, {@link JsonValueTypes#NULL}.
     */
    public static final EnumSet<JsonValueTypes> ARRAY_OR_NULL = EnumSet.of(ARRAY, NULL);

    static void checkAllowedTypeInvariants(EnumSet<JsonValueTypes> allowedTypes) {
        if (allowedTypes.isEmpty()) {
            throw new IllegalArgumentException("allowedTypes is empty");
        }
        if (allowedTypes.size() == 1 && allowedTypes.contains(JsonValueTypes.NULL)) {
            throw new IllegalArgumentException("allowedTypes is only accepting NULL");
        }
        if (allowedTypes.contains(JsonValueTypes.DECIMAL) && !allowedTypes.contains(JsonValueTypes.INT)) {
            throw new IllegalArgumentException("allowedTypes is accepting DECIMAL but not INT");
        }
    }
}
