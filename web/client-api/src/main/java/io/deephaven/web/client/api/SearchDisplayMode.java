//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@TsTypeDef(tsType = "string")
@JsType(namespace = "dh")
public class SearchDisplayMode {
    public static final String SEARCH_DISPLAY_DEFAULT = "Default",
            SEARCH_DISPLAY_HIDE = "Hide",
            SEARCH_DISPLAY_SHOW = "Show";
}
