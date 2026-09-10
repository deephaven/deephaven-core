//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import elemental2.core.ReadonlyArray;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByCumulativeCountWhere {
    @TsLiteral
    public final String type = "UpdateByCumulativeCountWhere";

    public String resultColumn;
    public ReadonlyArray<String> filters;// TODO
}
