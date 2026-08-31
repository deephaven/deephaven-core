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
 * Directives for how to handle {@code null} and {@code NaN} values.
 *
 * <ul>
 * <li>THROW - Throw an exception and abort processing when bad data is encountered.</li>
 * <li>RESET - Reset the state for the bucket to {@code null} when invalid data is encountered.</li>
 * <li>SKIP - Skip and do not process the invalid data without changing state.</li>
 * <li>POISON - Allow the bad data to poison the result. This is only valid for use with NaN.</li>
 * </ul>
 */
@TsName(namespace = "dh.updateby", name = "BadDataBehavior")
@TsUnion(anonymous = false)
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
