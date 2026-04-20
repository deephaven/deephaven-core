//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.DoubleRangeRestriction;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.IntegerRangeRestriction;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.NonEmptyRestriction;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.NotNullRestriction;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.inputtable_pb.StringListRestriction;
import io.deephaven.web.client.api.ColumnRestriction;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.base.Any;
import jsinterop.base.Js;

/**
 * Utility class for converting protobuf column restrictions to ColumnRestriction objects.
 */
public class ColumnRestrictionUtils {

    private ColumnRestrictionUtils() {
        // Utility class - no instances
    }

    /**
     * Extract the restriction type short name from a google.protobuf.Any object.
     *
     * @param restrictionAny The google.protobuf.Any object containing the restriction
     * @return The short type name (e.g., "IntegerRangeRestriction"), or null if not found
     */
    public static String getRestrictionType(Object restrictionAny) {
        String typeUrl = getTypeUrl(restrictionAny);
        if (typeUrl == null) {
            return null;
        }

        // Extract the restriction type from the type URL
        // Format: "type.googleapis.com/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
        // or "docs.deephaven.io/io.deephaven.proto.backplane.grpc.IntegerRangeRestriction"
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        return typeName.substring(typeName.lastIndexOf('.') + 1);
    }

    /**
     * Extract the type URL from a google.protobuf.Any object.
     */
    private static native String getTypeUrl(Object anyObj) /*-{
        if (!anyObj) return null;
        if (typeof anyObj.getTypeUrl === 'function') {
            return anyObj.getTypeUrl();
        }
        if (anyObj.type_url) {
            return anyObj.type_url;
        }
        return null;
    }-*/;

    /**
     * Extract the value bytes from a google.protobuf.Any object.
     */
    private static native Uint8Array getValue(Object anyObj) /*-{
        if (!anyObj) return null;
        if (typeof anyObj.getValue_asU8 === 'function') {
            return anyObj.getValue_asU8();
        }
        if (typeof anyObj.getValue === 'function') {
            var value = anyObj.getValue();
            if (value) return value;
        }
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

            IntegerRangeRestriction restriction = IntegerRangeRestriction.deserializeBinary(valueBytes);
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

            DoubleRangeRestriction restriction = DoubleRangeRestriction.deserializeBinary(valueBytes);
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

            NotNullRestriction.deserializeBinary(valueBytes); // Just to validate
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

            NonEmptyRestriction.deserializeBinary(valueBytes); // Just to validate
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

            StringListRestriction restriction = StringListRestriction.deserializeBinary(valueBytes);
            JsArray<String> allowedValues = restriction.getAllowedValuesList();

            // Cast JsArray<String> to JsArray<Any> to match constructor signature
            JsArray<Any> allowedValuesAsAny = Js.uncheckedCast(allowedValues);

            return new ColumnRestriction("StringListRestriction", allowedValuesAsAny);
        } catch (Exception e) {
            JsLog.warn("Failed to convert StringListRestriction:", e);
            return null;
        }
    }
}

