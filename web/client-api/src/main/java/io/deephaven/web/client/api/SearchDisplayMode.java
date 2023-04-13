package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@TsTypeDef(tsType = "string")
@JsType(namespace = "dh")
public class SearchDisplayMode {
    @JsProperty
    public static final String SEARCH_DISPLAY_DEFAULT = "Default",
            SEARCH_DISPLAY_HIDE = "Hide",
            SEARCH_DISPLAY_SHOW = "Show";
}