//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface UpdateByWindowScale {
    @TsUnionMember
    @JsOverlay
    default UpdateByWindowTicks asTicks() {
        return Js.uncheckedCast(this);
    }

    @TsUnionMember
    @JsOverlay
    default UpdateByWindowTime asTime() {
        return Js.uncheckedCast(this);
    }

    @JsProperty
    String getType();

    @JsOverlay
    default io.deephaven.proto.backplane.grpc.UpdateByWindowScale.Builder toProto() {
        io.deephaven.proto.backplane.grpc.UpdateByWindowScale.Builder b =
                io.deephaven.proto.backplane.grpc.UpdateByWindowScale.newBuilder();
        switch (getType()) {
            case "ticks":
                b.setTicks(io.deephaven.proto.backplane.grpc.UpdateByWindowScale.UpdateByWindowTicks.newBuilder()
                        .setTicks(asTicks().ticks));
                break;
            case "time":
                UpdateByWindowTime time = asTime();
                io.deephaven.proto.backplane.grpc.UpdateByWindowScale.UpdateByWindowTime.Builder tb =
                        io.deephaven.proto.backplane.grpc.UpdateByWindowScale.UpdateByWindowTime.newBuilder()
                                .setColumn(time.column.columnName());
                // duration can be a number (nanos), a string, or a LongWrapper
                if (Js.typeof(time.duration).equals("number")) {
                    tb.setNanos((long) time.duration.asNanosNumber());
                } else if (Js.typeof(time.duration).equals("string")) {
                    tb.setDurationString(time.duration.asDurationString());
                } else {
                    tb.setNanos(time.duration.asNanosLong().getWrapped());
                }
                b.setTime(tb);
                break;
            default:
                throw new IllegalArgumentException("Unsupported window scale type: " + getType());
        }
        return b;
    }
}
