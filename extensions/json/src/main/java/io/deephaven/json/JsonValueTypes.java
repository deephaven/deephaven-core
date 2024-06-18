//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * The JSON value types.
 */
public enum JsonValueTypes {
    /**
     * A JSON object type.
     */
    OBJECT,

    /**
     * A JSON array type.
     */
    ARRAY,

    /**
     * A JSON string type.
     */
    STRING,

    /**
     * A JSON number type without a decimal.
     */
    INT,

    /**
     * A JSON number type with a decimal.
     */
    DECIMAL,

    /**
     * The JSON literal 'true' or 'false' type.
     */
    BOOL,

    /**
     * The JSON literal 'null' type.
     */
    NULL;

    /**
     * An unmodifiable set of all {@link JsonValueTypes}.
     */
    public static Set<JsonValueTypes> all() {
        return ALL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}.
     */
    public static Set<JsonValueTypes> int_() {
        return INT_SET;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> intOrNull() {
        return INT_OR_NULL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}
     */
    public static Set<JsonValueTypes> intLike() {
        return INT_LIKE;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}.
     */
    public static Set<JsonValueTypes> number() {
        return NUMBER;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> numberOrNull() {
        return NUMBER_OR_NULL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL}, {@link JsonValueTypes#STRING},
     * {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> numberLike() {
        return NUMBER_LIKE;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#STRING}.
     */
    public static Set<JsonValueTypes> string() {
        return STRING_SET;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> stringOrNull() {
        return STRING_OR_NULL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#STRING}, {@link JsonValueTypes#INT}, {@link JsonValueTypes#DECIMAL},
     * {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> stringLike() {
        return STRING_LIKE;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#BOOL}.
     */
    public static Set<JsonValueTypes> bool() {
        return BOOL_SET;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> boolOrNull() {
        return BOOL_OR_NULL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#BOOL}, {@link JsonValueTypes#STRING}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> boolLike() {
        return BOOL_LIKE;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#OBJECT}.
     */
    public static Set<JsonValueTypes> object() {
        return OBJECT_SET;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#OBJECT}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> objectOrNull() {
        return OBJECT_OR_NULL;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#ARRAY}.
     */
    public static Set<JsonValueTypes> array() {
        return ARRAY_SET;
    }

    /**
     * An unmodifiable set of {@link JsonValueTypes#ARRAY}, {@link JsonValueTypes#NULL}.
     */
    public static Set<JsonValueTypes> arrayOrNull() {
        return ARRAY_OR_NULL;
    }

    static void checkAllowedTypeInvariants(Set<JsonValueTypes> allowedTypes) {
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

    private static final Set<JsonValueTypes> ALL = Collections.unmodifiableSet(EnumSet.allOf(JsonValueTypes.class));

    private static final Set<JsonValueTypes> INT_SET = Collections.unmodifiableSet(EnumSet.of(INT));

    private static final Set<JsonValueTypes> INT_OR_NULL = Collections.unmodifiableSet(EnumSet.of(INT, NULL));

    private static final Set<JsonValueTypes> INT_LIKE = Collections.unmodifiableSet(EnumSet.of(INT, STRING, NULL));

    private static final Set<JsonValueTypes> NUMBER = Collections.unmodifiableSet(EnumSet.of(INT, DECIMAL));

    private static final Set<JsonValueTypes> NUMBER_OR_NULL =
            Collections.unmodifiableSet(EnumSet.of(INT, DECIMAL, NULL));

    private static final Set<JsonValueTypes> NUMBER_LIKE =
            Collections.unmodifiableSet(EnumSet.of(INT, DECIMAL, STRING, NULL));

    private static final Set<JsonValueTypes> STRING_SET = Collections.unmodifiableSet(EnumSet.of(STRING));

    private static final Set<JsonValueTypes> STRING_OR_NULL = Collections.unmodifiableSet(EnumSet.of(STRING, NULL));

    private static final Set<JsonValueTypes> STRING_LIKE =
            Collections.unmodifiableSet(EnumSet.of(STRING, INT, DECIMAL, BOOL, NULL));

    private static final Set<JsonValueTypes> BOOL_SET = Collections.unmodifiableSet(EnumSet.of(BOOL));

    private static final Set<JsonValueTypes> BOOL_OR_NULL = Collections.unmodifiableSet(EnumSet.of(BOOL, NULL));

    private static final Set<JsonValueTypes> BOOL_LIKE = Collections.unmodifiableSet(EnumSet.of(BOOL, STRING, NULL));

    private static final Set<JsonValueTypes> OBJECT_SET = Collections.unmodifiableSet(EnumSet.of(OBJECT));

    private static final Set<JsonValueTypes> OBJECT_OR_NULL = Collections.unmodifiableSet(EnumSet.of(OBJECT, NULL));

    private static final Set<JsonValueTypes> ARRAY_SET = Collections.unmodifiableSet(EnumSet.of(ARRAY));

    private static final Set<JsonValueTypes> ARRAY_OR_NULL = Collections.unmodifiableSet(EnumSet.of(ARRAY, NULL));
}
