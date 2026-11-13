//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.JsTable;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * One operation in a {@link io.deephaven.web.client.api.JsTableOperations#updateBy(UpdateByOptions)} call: what to
 * compute, and the columns to compute it on.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByOperation {
    /**
     * The operation to perform. Its {@code type} field selects which one.
     */
    public UpdateBySpecUnion spec;

    /**
     * The columns to operate on, each a column, a column name, or a renaming expression such as {@code "Out = In"}.
     * Each produces one output column, replacing any existing column of that name. Output names must be unique across
     * all operations in the call. If unset or empty, the operation replaces every column of a type it supports, except
     * the {@link UpdateByOptions#groupByColumns} and, for a time-based operation other than
     * {@link UpdateByRollingGroup}, the timestamp column. {@link UpdateByCumulativeCountWhere},
     * {@link UpdateByRollingCountWhere} and {@link UpdateByRollingFormula} ignore this, since they name their own
     * output column.
     */
    @JsNullable
    public ReadonlyArray<JsTable.MatchPairUnion> matchPairs;
}
