//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Collects each input column's values in each row's window into an array.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingGroup extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingGroup";
}
