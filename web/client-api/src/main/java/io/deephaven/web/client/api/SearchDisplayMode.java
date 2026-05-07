//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Display mode values for the table search bar.
 */
@TsTypeDef(tsType = "string")
@JsType(namespace = "dh")
public class SearchDisplayMode {
    /**
     * Use the system default search bar display mode.
     */
    public static final String SEARCH_DISPLAY_DEFAULT = "Default";

    /**
     * Hide the search bar.
     */
    public static final String SEARCH_DISPLAY_HIDE = "Hide";

    /**
     * Show the search bar.
     */
    public static final String SEARCH_DISPLAY_SHOW = "Show";
}
