//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.JsRangeSet;
import io.deephaven.web.client.api.TableData;
import jsinterop.annotations.JsProperty;

/**
 * Event data, describing the indexes that were added/removed/updated, and providing access to Rows (and thus data in
 * columns) either by index, or scanning the complete present index.
 * <p>
 * This class supports two ways of reading the table - checking the changes made since the last update, and reading all
 * data currently in the table. While it is more expensive to always iterate over every single row in the table, it may
 * in some cases actually be cheaper than maintaining state separately and updating only the changes, though both
 * options should be considered.
 * <p>
 * The RangeSet objects allow iterating over the LongWrapper indexes in the table. Note that these "indexes" are not
 * necessarily contiguous and may be negative, and represent some internal state on the server, allowing it to keep
 * track of data efficiently. Those LongWrapper objects can be passed to the various methods on this instance to read
 * specific rows or cells out of the table.
 */
@TsInterface
@TsName(name = "SubscriptionTableData", namespace = "dh")
public interface SubscriptionTableData extends TableData {


    @JsProperty
    JsRangeSet getFullIndex();

    /**
     * The ordered set of row indexes added since the last update.
     *
     * @return the rangeset of rows added
     */
    @JsProperty
    JsRangeSet getAdded();

    /**
     * The ordered set of row indexes removed since the last update
     *
     * @return the rangeset of removed rows
     */
    @JsProperty
    JsRangeSet getRemoved();

    /**
     * The ordered set of row indexes updated since the last update
     *
     * @return the rnageset of modified rows
     */
    @JsProperty
    JsRangeSet getModified();
}
