//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for column restriction converters. Built-in restriction types are handled by typed subclasses of
 * {@link io.deephaven.web.client.api.ColumnRestriction} and are pre-registered here. Downstream consumers can register
 * additional custom types via {@link #register}; their converter must return a concrete subclass that overrides
 * {@code validate()} as needed.
 */
public class ColumnRestrictionRegistry {

    private static final Map<String, ColumnRestrictionConverter> converters = new HashMap<>();

    static {
        converters.put("IntegerRangeRestriction", ColumnRestrictionUtils::convertIntegerRangeRestriction);
        converters.put("DoubleRangeRestriction", ColumnRestrictionUtils::convertDoubleRangeRestriction);
        converters.put("NotNullRestriction", ColumnRestrictionUtils::convertNotNullRestriction);
        converters.put("NonEmptyRestriction", ColumnRestrictionUtils::convertNonEmptyRestriction);
        converters.put("StringListRestriction", ColumnRestrictionUtils::convertStringListRestriction);
    }

    /**
     * Register a converter for a custom column restriction type. The converter is responsible for returning a concrete
     * {@link io.deephaven.web.client.api.ColumnRestriction} subclass that implements {@code validate()} as needed.
     *
     * @param restrictionType The restriction type name (e.g., "MyCustomRestriction")
     * @param converter Converts a protobuf {@code Any} message into a typed {@code ColumnRestriction}
     */
    public static void register(String restrictionType, ColumnRestrictionConverter converter) {
        converters.put(restrictionType, converter);
    }

    public static ColumnRestrictionConverter getConverter(String restrictionType) {
        return converters.get(restrictionType);
    }
}
