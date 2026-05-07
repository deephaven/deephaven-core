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
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.nio.ByteBuffer;

/**
 * Utility class for converting protobuf column restrictions to {@link ColumnRestriction} objects, and for providing
 * built-in client-side validators for each known restriction type.
 */
public class ColumnRestrictionUtils {

    private ColumnRestrictionUtils() {
        // Utility class - no instances
    }

    /**
     * Extract the restriction type short name from a google.protobuf.Any type URL.
     *
     * @param typeUrl The type URL (e.g.,
     *        "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction")
     * @return The short type name (e.g., "IntegerRangeRestriction")
     */
    public static String getRestrictionType(String typeUrl) {
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        return typeName.substring(typeName.lastIndexOf('.') + 1);
    }

    // -------------------------------------------------------------------------
    // Parsers (protobuf Any -> ColumnRestriction)
    // -------------------------------------------------------------------------

    /**
     * Convert an {@code IntegerRangeRestriction} protobuf {@code Any} message into a {@link ColumnRestriction} with
     * type {@code "IntegerRangeRestriction"} and data {@code {min: number, max: number}}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A {@link ColumnRestriction} representing the integer range restriction
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertIntegerRangeRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            IntegerRangeRestriction restriction = IntegerRangeRestriction.parseFrom(buffer);
            JsPropertyMap<Object> data = JsPropertyMap.of();
            data.set("min", restriction.hasMinInclusive() ? (double) restriction.getMinInclusive() : Double.NaN);
            data.set("max", restriction.hasMaxInclusive() ? (double) restriction.getMaxInclusive() : Double.NaN);
            return new ColumnRestriction("IntegerRangeRestriction", data);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert IntegerRangeRestriction", e);
        }
    }

    /**
     * Convert a {@code DoubleRangeRestriction} protobuf {@code Any} message into a {@link ColumnRestriction} with type
     * {@code "DoubleRangeRestriction"} and data {@code {min: number, max: number}}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A {@link ColumnRestriction} representing the double range restriction
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertDoubleRangeRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            DoubleRangeRestriction restriction = DoubleRangeRestriction.parseFrom(buffer);
            JsPropertyMap<Object> data = JsPropertyMap.of();
            data.set("min", restriction.hasMinInclusive() ? restriction.getMinInclusive() : Double.NaN);
            data.set("max", restriction.hasMaxInclusive() ? restriction.getMaxInclusive() : Double.NaN);
            return new ColumnRestriction("DoubleRangeRestriction", data);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert DoubleRangeRestriction", e);
        }
    }

    /**
     * Convert a {@code NotNullRestriction} protobuf {@code Any} message into a {@link ColumnRestriction} with type
     * {@code "NotNullRestriction"} and an empty data object.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A {@link ColumnRestriction} representing the not-null restriction
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertNotNullRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            NotNullRestriction.parseFrom(buffer);
            return new ColumnRestriction("NotNullRestriction", JsPropertyMap.of());
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert NotNullRestriction", e);
        }
    }

    /**
     * Convert a {@code NonEmptyRestriction} protobuf {@code Any} message into a {@link ColumnRestriction} with type
     * {@code "NonEmptyRestriction"} and an empty data object.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A {@link ColumnRestriction} representing the non-empty restriction
     * @throws ColumnRestrictionConverterException if the message cannot be parsed
     */
    public static ColumnRestriction convertNonEmptyRestriction(Any restrictionAny)
            throws ColumnRestrictionConverterException {
        try {
            ByteBuffer buffer = restrictionAny.getValue().asReadOnlyByteBuffer();
            NonEmptyRestriction.parseFrom(buffer);
            return new ColumnRestriction("NonEmptyRestriction", JsPropertyMap.of());
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert NonEmptyRestriction", e);
        }
    }

    /**
     * Convert a {@code StringListRestriction} protobuf {@code Any} message into a {@link ColumnRestriction} with type
     * {@code "StringListRestriction"} and data {@code {values: string[]}}.
     *
     * @param restrictionAny The packed protobuf {@code Any} message
     * @return A {@link ColumnRestriction} representing the string list restriction
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
            JsPropertyMap<Object> data = JsPropertyMap.of();
            data.set("values", values);
            return new ColumnRestriction("StringListRestriction", data);
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert StringListRestriction", e);
        }
    }

    // -------------------------------------------------------------------------
    // Built-in client-side validators
    // -------------------------------------------------------------------------

    /**
     * Validate that a numeric value falls within the integer range defined in the restriction data.
     *
     * @param value The proposed column value
     * @param data Restriction data with {@code min} and {@code max} fields (may be {@code NaN} if unbounded)
     * @return An error message if the value is out of range, or {@code null} if valid
     */
    public static String validateIntegerRange(Object value, JsPropertyMap<Object> data) {
        if (value == null)
            return null;
        double num = Js.<Double>cast(value);
        double min = Js.<Double>cast(data.get("min"));
        double max = Js.<Double>cast(data.get("max"));
        if (!Double.isNaN(min) && num < min) {
            return "Value " + num + " is less than the minimum allowed value of " + min;
        }
        if (!Double.isNaN(max) && num > max) {
            return "Value " + num + " is greater than the maximum allowed value of " + max;
        }
        return null;
    }

    /**
     * Validate that a numeric value falls within the double range defined in the restriction data.
     *
     * @param value The proposed column value
     * @param data Restriction data with {@code min} and {@code max} fields (may be {@code NaN} if unbounded)
     * @return An error message if the value is out of range, or {@code null} if valid
     */
    public static String validateDoubleRange(Object value, JsPropertyMap<Object> data) {
        return validateIntegerRange(value, data); // same numeric logic
    }

    /**
     * Validate that a value is not {@code null}.
     *
     * @param value The proposed column value
     * @param data Restriction data (unused for this restriction type)
     * @return An error message if the value is {@code null}, or {@code null} if valid
     */
    public static String validateNotNull(Object value, JsPropertyMap<Object> data) {
        return value == null ? "Value must not be null" : null;
    }

    /**
     * Validate that a string value is neither {@code null} nor empty.
     *
     * @param value The proposed column value
     * @param data Restriction data (unused for this restriction type)
     * @return An error message if the value is {@code null} or empty, or {@code null} if valid
     */
    public static String validateNonEmpty(Object value, JsPropertyMap<Object> data) {
        if (value == null)
            return "Value must not be null or empty";
        String str = Js.cast(value);
        return str.isEmpty() ? "Value must not be empty" : null;
    }

    /**
     * Validate that a string value is one of the allowed values defined in the restriction data.
     *
     * @param value The proposed column value
     * @param data Restriction data with a {@code values} field containing the array of allowed strings
     * @return An error message if the value is not in the allowed list, or {@code null} if valid
     */
    public static String validateStringList(Object value, JsPropertyMap<Object> data) {
        if (value == null)
            return null;
        String str = Js.cast(value);
        JsArray<String> allowed = Js.cast(data.get("values"));
        for (int i = 0; i < allowed.length; i++) {
            if (allowed.getAt(i).equals(str))
                return null;
        }
        return "Value '" + str + "' is not in the allowed list: " + allowed.join(", ");
    }
}
