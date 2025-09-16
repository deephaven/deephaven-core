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
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Options for requesting table data, either as a snapshot or a subscription. See subtypes for more specific options:
 * <ul>
 * <li>{@link SnapshotOptions} for snapshot-specific options</li>
 * <li>{@link SubscriptionOptions} for full-table subscription-specific options</li>
 * <li>{@link ViewportSubscriptionOptions} for viewport subscription-specific options</li>
 * </ul>
 */
@TsInterface
@TsName(namespace = "dh")
public class DataOptions {
    /**
     * Options for requesting a preview of columns when requesting table data. The two properties
     * {@link #convertArrayToString} and {@link #array} are mutually exclusive.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class PreviewOptions {
        /**
         * Creates a deep copy of the object as the expected Java type.
         */
        @JsIgnore
        public static PreviewOptions of(JsPropertyMap<Object> previewOptions) {
            PreviewOptions options = new PreviewOptions();
            if (previewOptions.has("convertArrayToString")) {
                options.convertArrayToString = Js.asBoolean(previewOptions.get("convertArrayToString"));
            }
            if (previewOptions.has("array")) {
                options.array = Js.cast(previewOptions.get("array"));
            }
            return options;
        }

        /**
         * If true, any array columns will be converted to strings for preview purposes. This is the legacy behavior.
         */
        @JsNullable
        @JsProperty
        public Boolean convertArrayToString;

        /**
         * Specifies the length of the array to serialize. If the array is longer than this length, one extra element
         * will be included - the client is expected to truncate the array and append "..." to indicate that there are
         * additional elements. Negative values indicate that the values should be counted from the end of the array
         * rather than the beginning. Zero or null indicate that the entire array should be serialized. The default is
         * null.
         */
        @JsNullable
        @JsProperty
        public Double array;
    }

    /**
     * Optional preview instructions for columns in the table data.
     */
    @JsProperty
    @JsNullable
    public PreviewOptions previewOptions;

    /**
     * Required property to indicate which columns should be included in the table data.
     */
    @JsProperty
    public JsArray<Column> columns;

    /**
     * Options for requesting a full-table subscription to a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class SubscriptionOptions extends DataOptions {
        /**
         * Creates a deep copy of the object as the expected Java type.
         */
        @JsIgnore
        public static SubscriptionOptions of(Object obj) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            JsPropertyMap<Any> config = (JsPropertyMap) Js.asPropertyMap(obj);
            SubscriptionOptions options = new SubscriptionOptions();
            if (config.has("previewOptions")) {
                options.previewOptions = PreviewOptions.of(config.get("previewOptions").asPropertyMap());
            }
            if (config.has("columns")) {
                options.columns = Js.<JsArray<Column>>uncheckedCast(config.get("columns")).slice();
            } else {
                throw new IllegalArgumentException("Missing 'columns' property in subscription options");
            }
            if (config.has("updateIntervalMs")) {
                options.updateIntervalMs = Js.cast(config.get("updateIntervalMs"));
            }
            return options;
        }

        /**
         * Minimum interval between updates, in milliseconds. If not specified, the server default will be used,
         * typically 1000ms.
         * <p>
         * Note that setting this to smaller values will not necessarily result in more frequent updates - the server
         * may not propagate updates that frequently, or there may be no updates to propagate.
         */
        @JsProperty
        @JsNullable
        public Double updateIntervalMs;
    }

    /**
     * Options for requesting a viewport subscription to a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class ViewportSubscriptionOptions extends SubscriptionOptions {
        /**
         * Creates a deep copy of the object as the expected Java type.
         */
        @JsIgnore
        public static ViewportSubscriptionOptions of(Object obj) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            JsPropertyMap<Any> config = (JsPropertyMap) Js.asPropertyMap(obj);
            ViewportSubscriptionOptions options = new ViewportSubscriptionOptions();
            if (config.has("previewOptions")) {
                options.previewOptions = PreviewOptions.of(config.get("previewOptions").asPropertyMap());
            }
            JsArray<Column> columns = Js.uncheckedCast(config.get("columns"));
            if (columns != null) {
                options.columns = columns.slice();
            } // Skipping a null check for columns, we still allow legacy cases where no columns are specified

            if (config.has("updateIntervalMs")) {
                options.updateIntervalMs = Js.cast(config.get("updateIntervalMs"));
            }
            if (config.has("isReverseViewport")) {
                options.isReverseViewport = Js.cast(config.get("isReverseViewport"));
            }
            if (config.has("rows")) {
                options.rows = Js.uncheckedCast(Js.<RangeSetUnion>uncheckedCast(config.get("rows")).asRangeSet());
            } else {
                throw new IllegalArgumentException("Missing 'rows' property in viewport subscription options");
            }
            return options;
        }

        /**
         * If true, the viewport will be filled starting from the end of the table, where 0 is the last row of the
         * table. Default is false.
         */
        @JsNullable
        @JsProperty
        public Boolean isReverseViewport;

        /**
         * The rows to include in the viewport. This can be either a {@link JsRangeSet} or a single range with
         * {@code first} and {@code last} properties.
         */
        @JsProperty
        public RangeSetUnion rows;
    }

    /**
     * Options for requesting a snapshot of a table.
     */
    @TsInterface
    @TsName(namespace = "dh")
    public static class SnapshotOptions extends DataOptions {
        /**
         * Creates a deep copy of the object as the expected Java type.
         */
        @JsIgnore
        public static SnapshotOptions of(Object obj) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            JsPropertyMap<Any> config = (JsPropertyMap) Js.asPropertyMap(obj);
            SnapshotOptions options = new SnapshotOptions();
            if (config.has("previewOptions")) {
                options.previewOptions = PreviewOptions.of(config.get("previewOptions").asPropertyMap());
            }
            if (config.has("columns")) {
                options.columns = Js.<JsArray<Column>>uncheckedCast(config.get("columns")).slice();
            } else {
                throw new IllegalArgumentException("Missing 'columns' property in snapshot options");
            }
            if (config.has("isReverseViewport")) {
                options.isReverseViewport = Js.cast(config.get("isReverseViewport"));
            }
            if (config.has("rows")) {
                options.rows = Js.uncheckedCast(Js.<RangeSetUnion>uncheckedCast(config.get("rows")).asRangeSet());
            } else {
                throw new IllegalArgumentException("Missing 'rows' property in snapshot options");
            }
            return options;
        }

        /**
         * If true, the snapshot will be filled starting from the end of the table, where 0 is the last row of the
         * table. Default is false.
         */
        @JsNullable
        @JsProperty
        public Boolean isReverseViewport;

        /**
         * The rows to include in the snapshot. This can be either a {@link JsRangeSet} or a single range with
         * {@code first} and {@code last} properties.
         */
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
            if (r.first == null || r.last == null) {
                throw new IllegalArgumentException("SimpleRange must have both 'first' and 'last' properties");
            }
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
        public Double first;
        public Double last;
    }
}
