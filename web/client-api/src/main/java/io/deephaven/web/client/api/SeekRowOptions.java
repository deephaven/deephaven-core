//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;

/**
 * Options for {@link JsResolvedTable#seekRow(SeekRowOptions)}: the value to find, and where and how to search for it.
 */
@TsInterface
@JsType(namespace = "dh")
public class SeekRowOptions {
    /**
     * The position of the row to search after. The search wraps around, so the row at this position is checked last.
     */
    public double startingRow;

    /**
     * The column, or the name of the column, to search.
     */
    public Column.ColumnOrName column;

    /**
     * The type to compare {@link #seekValue} as.
     */
    @TsTypeRef(ValueType.class)
    public String valueType;

    /**
     * The value to find.
     */
    public Any seekValue;

    /**
     * {@code true} to ignore case. Defaults to {@code false}.
     */
    @JsNullable
    public Boolean insensitive;

    /**
     * {@code true} to match values that contain {@link #seekValue} rather than equal it. Defaults to {@code false}.
     */
    @JsNullable
    public Boolean contains;

    /**
     * {@code true} to search backward. Defaults to {@code false}.
     */
    @JsNullable
    public Boolean isBackwards;
}
