//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@TsUnion
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface NullBehavior {
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String NULL_DOMINATES = "NULL_DOMINATES";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String VALUE_DOMINATES = "VALUE_DOMINATES";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String ZERO_DOMINATES = "ZERO_DOMINATES";
}
