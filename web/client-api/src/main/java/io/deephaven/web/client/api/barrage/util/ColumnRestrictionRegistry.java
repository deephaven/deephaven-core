//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for column restriction converters and validators. Supports built-in types and allows downstream consumers to
 * extend the restriction system with their own types via {@link #register}.
 */
public class ColumnRestrictionRegistry {

    private static final Map<String, ColumnRestrictionConverter> converters = new HashMap<>();
    private static final Map<String, ColumnRestrictionValidator> validators = new HashMap<>();

    static {
        register("IntegerRangeRestriction",
                ColumnRestrictionUtils::convertIntegerRangeRestriction,
                ColumnRestrictionUtils::validateIntegerRange);
        register("DoubleRangeRestriction",
                ColumnRestrictionUtils::convertDoubleRangeRestriction,
                ColumnRestrictionUtils::validateDoubleRange);
        register("NotNullRestriction",
                ColumnRestrictionUtils::convertNotNullRestriction,
                ColumnRestrictionUtils::validateNotNull);
        register("NonEmptyRestriction",
                ColumnRestrictionUtils::convertNonEmptyRestriction,
                ColumnRestrictionUtils::validateNonEmpty);
        register("StringListRestriction",
                ColumnRestrictionUtils::convertStringListRestriction,
                ColumnRestrictionUtils::validateStringList);
    }

    /**
     * Register a converter and optional client-side validator for a column restriction type. Calling this from
     * JavaScript allows downstream consumers to extend the restriction system with their own types.
     *
     * <p>
     * The {@code converter} converts a raw protobuf {@code Any} message into a
     * {@link io.deephaven.web.client.api.ColumnRestriction}. The {@code validator} (if provided) is a function that
     * takes a proposed value and the restriction's data object and returns a human-readable error message if the value
     * is invalid, or {@code null} if it is valid.
     *
     * @param restrictionType The restriction type name (e.g., "IntegerRangeRestriction")
     * @param converter Converts protobuf bytes into a ColumnRestriction
     * @param validator Optional client-side validation function; may be {@code null}
     */
    public static void register(String restrictionType, ColumnRestrictionConverter converter,
            ColumnRestrictionValidator validator) {
        converters.put(restrictionType, converter);
        if (validator != null) {
            validators.put(restrictionType, validator);
        }
    }

    public static ColumnRestrictionConverter getConverter(String restrictionType) {
        return converters.get(restrictionType);
    }

    public static ColumnRestrictionValidator getValidator(String restrictionType) {
        return validators.get(restrictionType);
    }
}

