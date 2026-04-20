//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.proto.backplane.grpc.NonEmptyRestriction;
import io.deephaven.proto.backplane.grpc.NotNullRestriction;
import io.deephaven.proto.backplane.grpc.DoubleRangeRestriction;
import io.deephaven.proto.backplane.grpc.IntegerRangeRestriction;
import io.deephaven.proto.backplane.grpc.StringListRestriction;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.base.Any;
import jsinterop.base.Js;
import org.gwtproject.nio.TypedArrayHelper;

import java.nio.ByteBuffer;

/**
 * Utility class for converting protobuf column restrictions to ColumnRestriction objects.
 */
public class ColumnRestrictionUtils {

    private ColumnRestrictionUtils() {
        // Utility class - no instances
    }

    /**
     * Extract the restriction type short name from a google.protobuf.Any object type url.
     *
     * @param typeUrl The type URL from the Any object (e.g., "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction")
     * @return The short type name (e.g., "IntegerRangeRestriction"), or null if not found
     */
    public static String getRestrictionType(String typeUrl) {
        // Extract the restriction type from the type URL
        // Format: "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
        // or "docs.deephaven.io/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        return typeName.substring(typeName.lastIndexOf('.') + 1);
    }

    /**
     * Extract the value bytes from a google.protobuf.Any object.
     */
    private static native Uint8Array getValue(Object anyObj) /*-{
        if (!anyObj) return null;

        // Try Java protobuf structure (value_.bytes_0)
        if (anyObj.value_ && anyObj.value_.bytes_0) {
            // Convert byte array to Uint8Array
            var bytes = anyObj.value_.bytes_0;
            if (Array.isArray(bytes)) {
                return new Uint8Array(bytes);
            }
            if (bytes instanceof Uint8Array) {
                return bytes;
            }
        }

        // Try method accessor
        if (typeof anyObj.getValue_asU8 === 'function') {
            return anyObj.getValue_asU8();
        }
        if (typeof anyObj.getValue === 'function') {
            var value = anyObj.getValue();
            if (value) return value;
        }

        // Try direct value field
        if (anyObj.value && anyObj.value instanceof Uint8Array) {
            return anyObj.value;
        }

        return null;
    }-*/;

    /**
     * Convert IntegerRangeRestriction data into a ColumnRestriction object.
     */
    public static ColumnRestriction convertIntegerRangeRestriction(Any restrictionAny) {
        try {
            Uint8Array valueBytes = getValue(Js.cast(restrictionAny));
            if (valueBytes == null) {
                JsLog.warn("No value bytes found in IntegerRangeRestriction Any object");
                return null;
            }

            ByteBuffer buffer = TypedArrayHelper.wrap(valueBytes);
            IntegerRangeRestriction restriction = IntegerRangeRestriction.parseFrom(buffer);
            double minValue = restriction.hasMinInclusive() ? restriction.getMinInclusive() : Double.NaN;
            double maxValue = restriction.hasMaxInclusive() ? restriction.getMaxInclusive() : Double.NaN;

            return new ColumnRestriction("IntegerRangeRestriction", minValue, maxValue);
        } catch (Exception e) {
            JsLog.warn("Failed to convert IntegerRangeRestriction:", e);
            return null;
        }
    }

    /**
     * Convert DoubleRangeRestriction data into a ColumnRestriction object.
     */
    public static ColumnRestriction convertDoubleRangeRestriction(Any restrictionAny) {
        try {
            Uint8Array valueBytes = getValue(Js.cast(restrictionAny));
            if (valueBytes == null) {
                JsLog.warn("No value bytes found in DoubleRangeRestriction Any object");
                return null;
            }

            ByteBuffer buffer = TypedArrayHelper.wrap(valueBytes);
            DoubleRangeRestriction restriction = DoubleRangeRestriction.parseFrom(buffer);
            double minValue = restriction.hasMinInclusive() ? restriction.getMinInclusive() : Double.NaN;
            double maxValue = restriction.hasMaxInclusive() ? restriction.getMaxInclusive() : Double.NaN;

            return new ColumnRestriction("DoubleRangeRestriction", minValue, maxValue);
        } catch (Exception e) {
            JsLog.warn("Failed to convert DoubleRangeRestriction:", e);
            return null;
        }
    }

    /**
     * Convert NotNullRestriction data into a ColumnRestriction object.
     */
    public static ColumnRestriction convertNotNullRestriction(Any restrictionAny) {
        try {
            Uint8Array valueBytes = getValue(Js.cast(restrictionAny));
            if (valueBytes == null) {
                JsLog.warn("No value bytes found in NotNullRestriction Any object");
                return null;
            }

            ByteBuffer buffer = TypedArrayHelper.wrap(valueBytes);
            NotNullRestriction.parseFrom(buffer); // Just to validate
            return new ColumnRestriction("NotNullRestriction");
        } catch (Exception e) {
            JsLog.warn("Failed to convert NotNullRestriction:", e);
            return null;
        }
    }

    /**
     * Convert NonEmptyRestriction data into a ColumnRestriction object.
     */
    public static ColumnRestriction convertNonEmptyRestriction(Any restrictionAny) {
        try {
            Uint8Array valueBytes = getValue(Js.cast(restrictionAny));
            if (valueBytes == null) {
                JsLog.warn("No value bytes found in NonEmptyRestriction Any object");
                return null;
            }

            ByteBuffer buffer = TypedArrayHelper.wrap(valueBytes);
            NonEmptyRestriction.parseFrom(buffer); // Just to validate
            return new ColumnRestriction("NonEmptyRestriction");
        } catch (Exception e) {
            JsLog.warn("Failed to convert NonEmptyRestriction:", e);
            return null;
        }
    }

    /**
     * Convert StringListRestriction data into a ColumnRestriction object.
     */
    public static ColumnRestriction convertStringListRestriction(Any restrictionAny) {
        try {
            Uint8Array valueBytes = getValue(Js.cast(restrictionAny));
            if (valueBytes == null) {
                JsLog.warn("No value bytes found in StringListRestriction Any object");
                return null;
            }

            ByteBuffer buffer = TypedArrayHelper.wrap(valueBytes);
            StringListRestriction restriction = StringListRestriction.parseFrom(buffer);

            // Convert ProtocolStringList to JsArray<Any>
            JsArray<Any> allowedValuesAsAny = new JsArray<>();
            for (String value : restriction.getAllowedValuesList()) {
                allowedValuesAsAny.push(Js.cast(value));
            }

            return new ColumnRestriction("StringListRestriction", allowedValuesAsAny);
        } catch (Exception e) {
            JsLog.warn("Failed to convert StringListRestriction:", e);
            return null;
        }
    }
}

