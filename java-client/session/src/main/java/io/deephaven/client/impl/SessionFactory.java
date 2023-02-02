/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface SessionFactory {

    Session newSession();

    CompletableFuture<? extends Session> newSessionFuture();
}
