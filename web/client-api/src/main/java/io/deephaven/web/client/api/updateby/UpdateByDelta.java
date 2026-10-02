//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByDelta {
    @TsLiteral
    public final String type = "UpdateByDelta";

    /**
     * Describes how null/NaN should be handled:
     * <ul>
     * <li>NULL_DOMINATES - In the case of Current - null, the null dominates so Column[i] - null = null</li>
     * <li>VALUE_DOMINATES - In the case of Current - null, the current value dominates so Column[i] - null =
     * Column[i]</li>
     * <li>ZERO_DOMINATES - In the case of Current - null, return zero so Column[i] - null = 0</li>
     * </ul>
     */
    @JsNullable
    public NullBehavior nullBehavior;
}
