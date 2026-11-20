//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Computes the difference between each row's value and the previous row's value in its group. If the current value is
 * null, the result is null. If the previous value is null, or there is no previous row, {@link #nullBehavior} decides
 * the result.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByDelta {
    @TsLiteral
    public final String type = "UpdateByDelta";

    /**
     * The result when the previous value is null, or the row is the first in its group:
     * <ul>
     * <li>{@code NULL_DOMINATES} — null</li>
     * <li>{@code VALUE_DOMINATES} — the current value</li>
     * <li>{@code ZERO_DOMINATES} — zero</li>
     * </ul>
     * Defaults to {@code NULL_DOMINATES}.
     */
    @JsNullable
    public NullBehavior nullBehavior;
}
