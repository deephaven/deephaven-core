//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsRangeSet;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsInterface
@TsName(namespace = "dh")
public class DataOptions {
    /**
     * Options for requesting a preview of columns when requesting table data. The two properties
     * {@link #convertToString} and {@link #array} are mutually exclusive.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class PreviewOptions {
        @JsNullable
        @JsProperty
        public Boolean convertToString;
        @JsNullable
        @JsProperty
        public Double array;
    }

    @JsProperty
    @JsNullable
    public PreviewOptions previewOptions;
    @JsProperty
    public JsArray<Column> columns;

    /**
     * Options for requesting a full-table subscription to a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class SubscriptionOptions extends DataOptions {
        @JsProperty
        public Double updateIntervalMs;
    }

    /**
     * Options for requesting a viewport subscription to a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class ViewportSubscriptionOptions extends SubscriptionOptions {
        @JsNullable
        @JsProperty
        public Boolean isReverseViewport;

        @JsProperty
        public RangeSetUnion rows;
    }

    /**
     * Options for requesting a snapshot of a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class SnapshotOptions extends DataOptions {
        @JsNullable
        @JsProperty
        public Boolean isReverseViewport;

        @JsProperty
        public RangeSetUnion rows;
    }
    /**
     * Simple TS union that includes the rangeset and a simplified set that must contain exactly one range, with a start
     * and end property that are both JS numbers.
     */
    @TsUnion
    @JsType(namespace = JsPackage.GLOBAL, name = "?", isNative = true)
    public interface RangeSetUnion {
        @TsUnionMember
        @JsOverlay
        default JsRangeSet asRangeSet() {
            // This implementation is slightly an abuse of how unions are supposed to work - it not only returns
            // one of the members of the union, but also creates the instance if necessary.
            if (this instanceof JsRangeSet) {
                return (JsRangeSet) this;
            }
            final SimpleRange r = asSimpleRange();
            return JsRangeSet.ofRange(r.first, r.last);
        }

        @TsUnionMember
        @JsOverlay
        default SimpleRange asSimpleRange() {
            return Js.uncheckedCast(this);
        }
    }

    /**
     * Simple range consisting only of start and end numbers. Will be migrated to deephaven-core in the near future, and
     * subsequently removed from this the coreplus namespace.
     */
    @TsInterface
    @JsType(namespace = "dh")
    public static class SimpleRange {
        public double first;
        public double last;
    }
}
