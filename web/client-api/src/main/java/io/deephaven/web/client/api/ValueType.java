//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
@TsTypeDef(tsType = "string")
public class ValueType {
    public static final String STRING = "String";
    public static final String NUMBER = "Number";
    public static final String DOUBLE = "Double";
    public static final String LONG = "Long";
    public static final String DATETIME = "Datetime";
    public static final String BOOLEAN = "Boolean";
}
