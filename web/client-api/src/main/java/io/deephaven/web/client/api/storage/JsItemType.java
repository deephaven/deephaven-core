//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.storage;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.storage", name = "ItemType")
@TsTypeDef(tsType = "string")
public class JsItemType {
    public static final String DIRECTORY = "directory";

    public static final String FILE = "file";
}
