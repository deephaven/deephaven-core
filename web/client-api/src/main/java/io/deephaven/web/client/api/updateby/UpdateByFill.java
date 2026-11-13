//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Replaces each null value with the most recent non-null value before it in its group. Nulls before the first non-null
 * value stay null.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByFill {
    @TsLiteral
    public final String type = "UpdateByFill";
}
