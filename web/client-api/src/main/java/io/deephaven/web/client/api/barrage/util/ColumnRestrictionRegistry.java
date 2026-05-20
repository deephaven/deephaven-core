//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import io.deephaven.web.client.api.DoubleRangeColumnRestriction;
import io.deephaven.web.client.api.IntegerRangeColumnRestriction;
import io.deephaven.web.client.api.NonEmptyColumnRestriction;
import io.deephaven.web.client.api.NotNullColumnRestriction;
import io.deephaven.web.client.api.StringListColumnRestriction;

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

    private static final String TYPE_URL_PREFIX =
            "docs.deephaven.io/io.deephaven.proto.backplane.grpc.";

    static {
        converters.put(TYPE_URL_PREFIX + "IntegerRangeRestriction", IntegerRangeColumnRestriction::fromAny);
        converters.put(TYPE_URL_PREFIX + "DoubleRangeRestriction", DoubleRangeColumnRestriction::fromAny);
        converters.put(TYPE_URL_PREFIX + "NotNullRestriction", NotNullColumnRestriction::fromAny);
        converters.put(TYPE_URL_PREFIX + "NonEmptyRestriction", NonEmptyColumnRestriction::fromAny);
        converters.put(TYPE_URL_PREFIX + "StringListRestriction", StringListColumnRestriction::fromAny);
    }

    /**
     * Register a converter for a custom column restriction type. The converter is responsible for returning a concrete
     * {@link io.deephaven.web.client.api.ColumnRestriction} subclass that implements {@code validate()} as needed.
     *
     * @param typeUrl The full protobuf {@code Any} type URL (e.g.,
     *        {@code "type.googleapis.com/io.deephaven.proto.backplane.grpc.MyCustomRestriction"})
     * @param converter Converts a protobuf {@code Any} message into a typed {@code ColumnRestriction}
     */
    public static void register(String typeUrl, ColumnRestrictionConverter converter) {
        converters.put(typeUrl, converter);
    }

    public static ColumnRestrictionConverter getConverter(String typeUrl) {
        return converters.get(typeUrl);
    }
}
