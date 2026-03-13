//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import jsinterop.annotations.JsType;

/**
 * A string union representing whether search UI elements should be shown.
 */
@TsTypeDef(tsType = "string")
@JsType(namespace = "dh")
public class SearchDisplayMode {
    /**
     * The supported values.
     *
     * <p>
     * {@link #SEARCH_DISPLAY_DEFAULT} uses the default behavior.
     * <p>
     * {@link #SEARCH_DISPLAY_HIDE} hides search UI elements.
     * <p>
     * {@link #SEARCH_DISPLAY_SHOW} shows search UI elements.
     */
    public static final String SEARCH_DISPLAY_DEFAULT = "Default",
            SEARCH_DISPLAY_HIDE = "Hide",
            SEARCH_DISPLAY_SHOW = "Show";
}
