//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsProperty;

/**
 * Extends {@link TableData}, but only contains data in the current viewport. The only API change from TableData is that
 * ViewportData also contains the offset to this data, so that the actual row number may be determined.
 * <p>
 * For viewport subscriptions, it is not necessary to read with the key, only with the position.
 * <p>
 * Do not assume that the first row in `rows` is the first visible row, because extra rows may be provided for easier
 * scrolling without going to the server.
 */
//TODO re-add dh.ViewportRow
@TsInterface
@TsName(namespace = "dh")
public interface ViewportData extends TableData {

    /**
     * The position of the first returned row, null if this data is not for a viewport.
     */
    @JsProperty
    Double getOffset();
}
