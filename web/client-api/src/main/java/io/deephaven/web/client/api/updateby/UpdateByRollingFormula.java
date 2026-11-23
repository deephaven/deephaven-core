//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes one output column from a formula written as {@code "Out = expression"}, over each row's window. Within the
 * expression, each input column stands for that column's values in the window, as a vector, so the expression applies
 * functions that reduce them, as in {@code "Total = sum(X)"} or {@code "Range = max(X) - min(X)"}. The
 * {@link UpdateByOptions#groupByColumns} are the exception: each stands for the group's single value.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingFormula extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingFormula";

    /**
     * The output column's name and the expression that computes it, as {@code "Out = expression"}.
     */
    public String formula;
}
