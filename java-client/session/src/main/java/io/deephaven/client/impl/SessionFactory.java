package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface SessionFactory {

    Session session();

    CompletableFuture<? extends Session> sessionFuture();
}
