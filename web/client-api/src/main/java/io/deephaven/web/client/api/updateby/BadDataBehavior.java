//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

/**
 * How an exponential moving operation handles a null or NaN value, or an invalid timestamp:
 *
 * <ul>
 * <li>THROW — throws an exception and fails the operation</li>
 * <li>RESET — resets the group's state to null</li>
 * <li>SKIP — ignores the value and keeps the current state</li>
 * <li>POISON — lets a NaN value propagate into the result, and is valid only for NaN</li>
 * </ul>
 */
@TsUnion(anonymous = false, namespace = "dh.updateby", name = "BadDataBehavior")
@JsType(name = "Object", namespace = JsPackage.GLOBAL, isNative = true)
public interface BadDataBehavior {
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String THROW = "THROW";
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String RESET = "RESET";
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String SKIP = "SKIP";
    @TsUnionMember
    @TsLiteral
    @JsOverlay
    String POISON = "POISON";
}
