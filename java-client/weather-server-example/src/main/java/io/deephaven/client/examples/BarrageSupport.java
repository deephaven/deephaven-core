package io.deephaven.client.examples;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSubscription;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is here to consolidate subscription management into a single place.
 */
public class BarrageSupport {
    private final BarrageSession session;
    private final Map<BarrageTable, BarrageSubscription> subscriptionMap = new HashMap<>();

    public BarrageSupport(BarrageSession session) {
        this.session = session;
    }

    /**
     * Fetch a properly subscribed BarrageTable for further use. All tables retrieved in this manner must be cleaned up
     * by a call to {@link #releaseTable(BarrageTable)} to ensure all server side resources are cleaned up.
     *
     * @param tableName the name of the binding table to fetch
     * @return a subscribed {@link BarrageTable}
     * @throws UncheckedDeephavenException if an error occurs during retrieval or subscription.
     */
    public BarrageTable fetchSubscribedTable(final @NotNull String tableName) throws UncheckedDeephavenException {
        final TableHandle handle = session.session().ticket(tableName);
        final BarrageSubscription sub = session.subscribe(handle, BarrageSubscriptionOptions.builder().build());
        final BarrageTable subscribedTable = sub.entireTable();

        synchronized (subscriptionMap) {
            subscriptionMap.put(subscribedTable, sub);
        }

        return subscribedTable;
    }

    /**
     * Release resources and unsubscribe from a {@link BarrageTable table} retrieved via a call to
     * {@link #fetchSubscribedTable(String)}.
     * 
     * @param table the table to release.
     */
    public void releaseTable(final @NotNull BarrageTable table) throws Exception {
        final BarrageSubscription sub;
        synchronized (subscriptionMap) {
            sub = subscriptionMap.remove(table);
        }

        if (sub == null) {
            throw new UncheckedDeephavenException("Table was already unsubscribed, or could not be found");
        }

        sub.close();
    }

    /**
     * Close and release -all- subscriptions held by this instance.
     */
    public void close() throws Exception {
        synchronized (subscriptionMap) {
            for (final BarrageSubscription sub : subscriptionMap.values()) {
                sub.close();
            }
        }
    }
}
