package io.deephaven.web.shared.data;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.BitSet;

/**
 * A class to encapsulate updates to a given table's subscription.
 *
 * Because the server is now managing the creation of a "tail table" to only subscribe to updates
 * after all filters have run, we are giving the server a bit more rights w.r.t. subscription state
 * management.
 *
 * The client will now send an array of ViewportSubscription, each containing a subscriptionId
 * (JsTable id) and a {@link Viewport} object.
 *
 * The server will be responsible for merging ranges and columns to create flattened tables with the
 * desired viewports.
 *
 * For now, we'll likely preserve "bunch them all together" semantics, but we should do performance
 * testing to identify if we'll get better performance from having multiple tables of smaller
 * viewport scope (more messages on the wire, but less work to do before sending messages).
 *
 * The {@link #columns} must be non-null (and almost always non-empty), but {@link #rows} may be
 * null to indicate a non-viewport subscription.
 */
public class TableSubscriptionRequest implements Serializable {

    private int subscriptionId;
    private RangeSet rows;
    private BitSet columns;

    public TableSubscriptionRequest() {

    }

    public TableSubscriptionRequest(int subscriptionId, @Nullable RangeSet rows, BitSet columns) {
        this.subscriptionId = subscriptionId;
        this.rows = rows;
        this.columns = columns;
    }

    public int getSubscriptionId() {
        return subscriptionId;
    }

    public RangeSet getRows() {
        return rows;
    }

    public BitSet getColumns() {
        return columns;
    }

    void setSubscriptionId(int subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    void setRows(RangeSet rows) {
        this.rows = rows;
    }

    void setColumns(BitSet columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "TableSubscriptionRequest{" +
            "subscriptionId=" + subscriptionId +
            ", rows=" + rows +
            ", columns=" + columns +
            '}';
    }
}
