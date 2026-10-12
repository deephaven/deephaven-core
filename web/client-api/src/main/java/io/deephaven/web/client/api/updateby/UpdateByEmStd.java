//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByEmStd {
    @TsLiteral
    public final String type = "UpdateByEmStd";
    @JsNullable
    public UpdateByEmOptions emOptions;
    public UpdateByWindowScale windowScale;
}
