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
public interface RoundingMode {
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String UP = "UP";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String DOWN = "DOWN";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String CEILING = "CEILING";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String FLOOR = "FLOOR";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String HALF_UP = "HALF_UP";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String HALF_DOWN = "HALF_DOWN";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String HALF_EVEN = "HALF_EVEN";

    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String UNNECESSARY = "UNNECESSARY";
}
