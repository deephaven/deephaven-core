//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import elemental2.core.ReadonlyArray;
import jsinterop.annotations.JsType;

/**
 * Counts the rows in each row's window that match all of the given filters.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingCountWhere extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingCountWhere";

    /** The name of the output column that holds the count. */
    public String resultColumn;
    /** Filter expressions, such as {@code "X > 5"}. The operation counts a row only if it matches all of them. */
    public ReadonlyArray<String> filters;// TODO
}
