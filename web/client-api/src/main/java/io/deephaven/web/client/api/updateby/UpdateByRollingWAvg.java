//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public final class UpdateByRollingWAvg extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingWAvg";
    public Column.ColumnOrName weightColumn;
}
