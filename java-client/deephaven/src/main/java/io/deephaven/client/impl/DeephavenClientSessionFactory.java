/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface DeephavenClientSessionFactory {
    DeephavenClientSession newDeephavenClientSession();

    CompletableFuture<? extends DeephavenClientSession> newDeephavenClientSessionFuture();
}
