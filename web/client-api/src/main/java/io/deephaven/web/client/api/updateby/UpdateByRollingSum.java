//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByRollingSum {
    @TsLiteral
    public final String type = "UpdateByRollingSum";
    public UpdateByWindowScale windowScale;
    public UpdateByWindowScale forwardWindowScale;
    public UpdateByWindowScale reverseWindowScale;
}
