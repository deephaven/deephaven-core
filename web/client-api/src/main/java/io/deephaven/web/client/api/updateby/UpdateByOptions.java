//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByOptions {
    @JsNullable
    public UpdateByControl control;

    public ReadonlyArray<UpdateByOperation> operations;

    @JsNullable
    public ReadonlyArray<Column.ColumnOrName> groupByColumns;
}
