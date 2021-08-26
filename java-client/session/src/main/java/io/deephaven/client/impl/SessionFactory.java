package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface SessionFactory {

    Session newSession();

    CompletableFuture<? extends Session> newSessionFuture();
}
