package io.deephaven.web.client.api.storage;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

//TODO this isn't working yet
@JsType(namespace = "dh.storage", name = "ItemType")
@TsTypeDef(tsType = "string")
public class JsItemType {
    public static final String DIRECTORY = "directory";

    public static final String FILE = "file";
}
