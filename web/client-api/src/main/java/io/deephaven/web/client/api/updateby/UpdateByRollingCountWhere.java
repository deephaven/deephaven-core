//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import elemental2.core.ReadonlyArray;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public final class UpdateByRollingCountWhere extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingCountWhere";

    public String resultColumn;
    public ReadonlyArray<String> filters;// TODO
}
