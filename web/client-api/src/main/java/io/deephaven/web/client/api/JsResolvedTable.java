//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

/**
 * A server-side table instance that the JS client has retained a reference to, and must be closed when no longer
 * needed. Provides access to the table's data and metadata. Supports all the methods of {@link JsTableOperations}, but
 * any returned table must again be awaited, closed when finished being used.
 */
@TsName(namespace = "dh", name = "ResolvedTable")
@TsInterface
public interface JsResolvedTable extends JsTableOperations {
    /**
     * The total count of rows in the table. If there is a viewport subscription active, this size will be updated when
     * the subscription updates. If not, and {@link #isUncoalesced()} is true, the size will be
     * {@link #SIZE_UNCOALESCED}. Otherwise, the size will be updated when the server's update graph processes changes.
     * <p>
     * When the size changes, the {@link #EVENT_SIZECHANGED} event will be fired.
     *
     * @return the size of the table, or {@link #SIZE_UNCOALESCED} if there is no subscription and the table is
     *         uncoalesced.
     */
    @JsProperty
    double getSize();

    /**
     * The columns that are present on this table.
     *
     * @return the columns present in this table
     */
    @JsProperty
    ReadonlyArray<Column> getColumns();

    @JsMethod
    Column findColumn(String columnName);

    /**
     * The names of all attributes defined on this table.
     *
     * @return an array of attributes defined on this table
     */
    @JsMethod
    ReadonlyArray<String> getAttributes();

    /**
     * {@code null} if no property exists, a string if it is an easily serializable property, or a {@code Promise
     * &lt;ResolvedTable&gt;} that will either resolve with a table or error out if the object can't be passed to JS.
     *
     * @param attributeName the name of the attribute to read
     * @return the value of the attribute or null if none with that name exists
     */
    @JsMethod
    Object getAttribute(String attributeName);

    @JsMethod
    Promise<TableData> createSnapshot(@TsTypeRef(DataOptions.SnapshotOptions.class) Object options);

    @JsMethod
    TableViewportSubscription createViewportSubscription(
            @TsTypeRef(DataOptions.ViewportSubscriptionOptions.class) Object options);

    @JsMethod
    TableSubscription createSubscription(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options);

    /**
     * Signals that the table will no longer be used and server resources should be cleaned up. Logs a warning if called
     * more than once.
     */
    // TODO support Symbol.dispose, if present
    @JsMethod
    void close();

    /**
     * Returns another instance of this table, allowing either instance to be closed without affecting the other.
     *
     * @return a copy of this table, sharing resources on the server
     */
    @JsMethod
    Promise<io.deephaven.web.client.api.JsResolvedTable> copy();
}
