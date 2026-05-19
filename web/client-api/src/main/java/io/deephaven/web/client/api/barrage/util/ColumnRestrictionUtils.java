//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import com.google.protobuf.Any;
import elemental2.core.JsArray;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.proto.backplane.grpc.IntegerRangeRestriction;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.api.DoubleRangeColumnRestriction;
import io.deephaven.web.client.api.IntegerRangeColumnRestriction;
import io.deephaven.web.client.api.NonEmptyColumnRestriction;
import io.deephaven.web.client.api.NotNullColumnRestriction;
import io.deephaven.web.client.api.StringListColumnRestriction;

import java.nio.ByteBuffer;

/**
 * Utility class for converting protobuf column restrictions to typed {@link ColumnRestriction} subclass instances.
 */
public class ColumnRestrictionUtils {

    private ColumnRestrictionUtils() {
        // Utility class - no instances
    }


    // -------------------------------------------------------------------------
    // Converters (protobuf Any -> typed ColumnRestriction subclass)
    // -------------------------------------------------------------------------

    /**
     * Convert an {@code IntegerRangeRestriction} protobuf {@code Any} message into an
     * {@link IntegerRangeColumnRestriction}. Either bound is {@code null} when absent (unbounded on that side).
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A typed {@link IntegerRangeColumnRestriction}
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertIntegerRangeRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            IntegerRangeRestriction restriction = IntegerRangeRestriction.parseFrom(buffer);
            return new IntegerRangeColumnRestriction(restriction);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert IntegerRangeRestriction", e);
        }
    }

    /**
     * Convert a {@code DoubleRangeRestriction} protobuf {@code Any} message into a
     * {@link DoubleRangeColumnRestriction}. Either bound is {@code null} when absent (unbounded on that side).
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A typed {@link DoubleRangeColumnRestriction}
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertDoubleRangeRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            DoubleRangeRestriction restriction = DoubleRangeRestriction.parseFrom(buffer);
            return new DoubleRangeColumnRestriction(restriction);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert DoubleRangeRestriction", e);
        }
    }

    /**
     * Convert a {@code NotNullRestriction} protobuf {@code Any} message into a {@link NotNullColumnRestriction}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A typed {@link NotNullColumnRestriction}
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertNotNullRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            NotNullRestriction.parseFrom(buffer);
            return new NotNullColumnRestriction();
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert NotNullRestriction", e);
        }
    }

    /**
     * Convert a {@code NonEmptyRestriction} protobuf {@code Any} message into a {@link NonEmptyColumnRestriction}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A typed {@link NonEmptyColumnRestriction}
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertNonEmptyRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            NonEmptyRestriction.parseFrom(buffer);
            return new NonEmptyColumnRestriction();
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert NonEmptyRestriction", e);
        }
    }

    /**
     * Convert a {@code StringListRestriction} protobuf {@code Any} message into a {@link StringListColumnRestriction}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A typed {@link StringListColumnRestriction}
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertStringListRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            StringListRestriction restriction = StringListRestriction.parseFrom(buffer);
            JsArray<String> values = new JsArray<>();
            for (String value : restriction.getAllowedValuesList()) {
                values.push(value);
            }
            return new StringListColumnRestriction(values);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert StringListRestriction", e);
        }
    }
}
