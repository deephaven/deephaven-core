//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import com.google.protobuf.Any;
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Registry for column restriction converters. Built-in restriction types are handled by typed subclasses of
 * {@link io.deephaven.web.client.api.ColumnRestriction} and are pre-registered here. Downstream consumers can register
 * additional custom types via {@link #register}; their converter must return a concrete subclass that overrides
 * {@code validate()} as needed.
 */
public class ColumnRestrictionRegistry {

    @FunctionalInterface
    private interface BufferParser<T extends ColumnRestriction> {
        T parse(ByteBuffer buffer) throws Exception;
    }

    private static final Map<String, ColumnRestrictionConverter> converters = new HashMap<>();

    private static final String TYPE_URL_PREFIX = "docs.deephaven.io/";

    static {
        register(TYPE_URL_PREFIX + "io.deephaven.proto.backplane.grpc.IntegerRangeRestriction",
                fromBuffer(buffer -> new IntegerRangeColumnRestriction(IntegerRangeRestriction.parseFrom(buffer))));
        register(TYPE_URL_PREFIX + "io.deephaven.proto.backplane.grpc.DoubleRangeRestriction",
                fromBuffer(buffer -> new DoubleRangeColumnRestriction(DoubleRangeRestriction.parseFrom(buffer))));
        register(TYPE_URL_PREFIX + "io.deephaven.proto.backplane.grpc.NotNullRestriction",
                fromBuffer(buffer -> new NotNullColumnRestriction(NotNullRestriction.parseFrom(buffer))));
        register(TYPE_URL_PREFIX + "io.deephaven.proto.backplane.grpc.NonEmptyRestriction",
                fromBuffer(buffer -> new NonEmptyColumnRestriction(NonEmptyRestriction.parseFrom(buffer))));
        register(TYPE_URL_PREFIX + "io.deephaven.proto.backplane.grpc.StringListRestriction",
                fromBuffer(buffer -> new StringListColumnRestriction(StringListRestriction.parseFrom(buffer))));
    }

    private static <T extends ColumnRestriction> ColumnRestrictionConverter fromBuffer(BufferParser<T> parser) {
        return restrictionAny -> {
            try {
                return parser.parse(restrictionAny.getValue().asReadOnlyByteBuffer());
            } catch (Exception e) {
                throw new ColumnRestrictionConverterException(
                        "Failed to convert " + restrictionAny.getTypeUrl(), e);
            }
        };
    }

    /**
     * Register a converter for a custom column restriction type. The converter is responsible for returning a concrete
     * {@link io.deephaven.web.client.api.ColumnRestriction} subclass that implements {@code validate()} as needed.
     *
     * @param typeUrl The full protobuf {@code Any} type URL (e.g.,
     *        {@code "docs.deephaven.io/io.deephaven.proto.backplane.grpc.MyCustomRestriction"})
     * @param converter Converts a protobuf {@code Any} message into a typed {@code ColumnRestriction}
     */
    public static void register(String typeUrl, ColumnRestrictionConverter converter) {
        converters.put(typeUrl, converter);
    }

    /**
     * Converts the given Any to a ColumnRestriction, if possible. Returns empty if the type is unknown, or throws if
     * the data is invalid for the given type.
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
}
