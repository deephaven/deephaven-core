//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public final class UpdateByRollingFormula extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingFormula";

    /**
     * The formula to use to calculate output values.
     */
    public String formula;
}
