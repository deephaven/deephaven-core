//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

public enum SubscriptionType {
    /** a subscription that will receive all updates to the table */
    FULL_SUBSCRIPTION,
    /** a subscription that will receive updates only for the current viewport */
    VIEWPORT_SUBSCRIPTION,
    /** a non-refreshing subscription that receives data once and then stops */
    SNAPSHOT
}
