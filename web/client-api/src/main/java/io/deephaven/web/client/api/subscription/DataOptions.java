package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsRangeSet;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

public class DataOptions {
    public static class PreviewOptions {
        @JsNullable
        public Boolean convertToString = false;
        @JsNullable
        public Double array;
    }
    @JsNullable
    public PreviewOptions previewOptions;
    public JsArray<Column> columns;

    public static class SubscriptionOptions extends DataOptions {
        public Double updateIntervalMs;
    }
    public static class ViewportSubscriptionOptions extends SubscriptionOptions {
        @JsNullable
        public Boolean isReverseViewport;

        public RangeSetUnion rows;
    }
    public static class SnapshotOptions extends DataOptions {
        @JsNullable
        public Boolean isReverseViewport;

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
