/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface BarrageSessionFactory {
    BarrageSession newBarrageSession();

    CompletableFuture<? extends BarrageSession> newBarrageSessionFuture();
}
