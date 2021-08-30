/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

public interface ItemUpdateListener {
    public void handleItemUpdated(Item item, long now, long appNow, int intervalIndex, long intervalMillis,
            String intervalName);

    public static final ItemUpdateListener NULL = new ItemUpdateListener() {
        public void handleItemUpdated(Item item, long now, long appNow, int intervalIndex, long intervalMillis,
                String intervalName) {
            // empty
        }
    };
}
