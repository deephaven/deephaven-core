//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Options to control a call to {@link io.deephaven.web.client.api.JsTableOperations#updateBy(UpdateByOptions)}.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByOptions {
    /**
     * Settings for how the server maintains the result. Any that are unset use the server's defaults.
     */
    @JsNullable
    public UpdateByControl control;

    /**
     * The operations to apply. Must not be empty.
     */
    public ReadonlyArray<UpdateByOperation> operations;

    /**
     * The columns to group by. Each operation runs separately within each group, in the table's row order. If empty or
     * unspecified, the whole table is one group.
     */
    @JsNullable
    public ReadonlyArray<Column.ColumnOrName> groupByColumns;
}
