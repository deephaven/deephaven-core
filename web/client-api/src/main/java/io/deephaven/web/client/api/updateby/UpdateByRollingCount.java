//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Counts the non-null values of each input column in each row's window.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingCount extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingCount";
}
