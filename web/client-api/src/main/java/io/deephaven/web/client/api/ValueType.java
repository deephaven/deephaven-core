//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Value type identifiers used in the Deephaven JS API.
 *
 * <p>
 * This is a string-union type exposed to JavaScript and TypeScript.
 */
@JsType(namespace = "dh")
@TsTypeDef(tsType = "string")
public class ValueType {
    /**
     * String values.
     */
    public static final String STRING = "String";
    /**
     * Numeric values.
     */
    public static final String NUMBER = "Number";
    /**
     * Double-precision floating point values.
     */
    public static final String DOUBLE = "Double";
    /**
     * 64-bit integer values.
     */
    public static final String LONG = "Long";
    /**
     * Date-time values.
     */
    public static final String DATETIME = "Datetime";
    /**
     * Boolean values.
     */
    public static final String BOOLEAN = "Boolean";
}
