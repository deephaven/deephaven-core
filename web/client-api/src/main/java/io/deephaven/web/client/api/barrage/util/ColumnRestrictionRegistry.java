//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.proto.backplane.grpc.IntegerRangeRestriction;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.api.DoubleRangeColumnRestriction;
import io.deephaven.web.client.api.IntegerRangeColumnRestriction;
import io.deephaven.web.client.api.NonEmptyColumnRestriction;
import io.deephaven.web.client.api.NotNullColumnRestriction;
import io.deephaven.web.client.api.StringListColumnRestriction;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Registry for column restriction converters. Built-in restriction types are handled by typed subclasses of
 * {@link io.deephaven.web.client.api.ColumnRestriction} and are pre-registered here. Downstream consumers can register
 * additional custom types via {@link #register}; their converter must return a concrete subclass that overrides
 * {@code validate()} as needed.
 */
public class ColumnRestrictionRegistry {

    private static final Map<String, ColumnRestrictionConverter> converters = new HashMap<>();

    private static final String TYPE_URL_PREFIX = "docs.deephaven.io/";

    static {
        register(TYPE_URL_PREFIX, IntegerRangeRestriction.getDefaultInstance(), IntegerRangeColumnRestriction::new);
        register(TYPE_URL_PREFIX, DoubleRangeRestriction.getDefaultInstance(), DoubleRangeColumnRestriction::new);
        register(TYPE_URL_PREFIX, NotNullRestriction.getDefaultInstance(), NotNullColumnRestriction::new);
        register(TYPE_URL_PREFIX, NonEmptyRestriction.getDefaultInstance(), NonEmptyColumnRestriction::new);
        register(TYPE_URL_PREFIX, StringListRestriction.getDefaultInstance(), StringListColumnRestriction::new);
    }

    /**
     * Register a converter for a custom column restriction type. The converter is responsible for returning a concrete
     * {@link io.deephaven.web.client.api.ColumnRestriction} subclass that implements {@code validate()} as needed.
     *
     * @param prefix The URL prefix to use for the given message type
     * @param message An empty message of the expected type, used to read the type name and show how to decode it
     * @param constructor Converts a typed protobuf message into a typed {@code ColumnRestriction}
     */
    public static <M extends Message> void register(String prefix, M message, Function<M, ColumnRestriction> constructor) {
        converters.put(prefix + message.getDescriptorForType().getFullName(), parser(message, constructor));
    }

    /**
     * Converts the given Any to a ColumnRestriction, if possible. Returns empty if the type is unknown, or throws
     * if the data is invalid for the given type.
     *
     * @param restrictionAny The Any restriction to convert
     * @return An optional containing the converted ColumnRestriction, or empty if the type is unknown
     * @throws ColumnRestrictionConverterException if the data cannot be converted
     */
    public static Optional<ColumnRestriction> convert(Any restrictionAny) throws ColumnRestrictionConverterException {
        ColumnRestrictionConverter converter = converters.get(restrictionAny.getTypeUrl());
        if (converter == null) {
            return Optional.empty();
        }
        return Optional.of(converter.convert(restrictionAny));
    }

    private static <M extends Message, T extends ColumnRestriction> ColumnRestrictionConverter parser(M message, Function<M, ColumnRestriction> constructor) {
        return restrictionAny -> {
            try {
                return constructor.apply(restrictionAny.unpackSameTypeAs(message));
            } catch (Exception e) {
                throw new ColumnRestrictionConverterException("Failed to convert " + message.getDescriptorForType().getFullName(), e);
            }
        };
    }
}
