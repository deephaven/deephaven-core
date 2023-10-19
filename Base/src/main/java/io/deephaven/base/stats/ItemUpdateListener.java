/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.stats;

public interface ItemUpdateListener {

    void handleItemUpdated(
            Item<?> item, long now, long appNow, int intervalIndex, long intervalMillis, String intervalName);

    ItemUpdateListener NULL = (item, now, appNow, intervalIndex, intervalMillis, intervalName) -> {
    };
}
