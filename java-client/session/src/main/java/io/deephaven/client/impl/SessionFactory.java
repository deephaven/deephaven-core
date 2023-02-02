/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.proto.DeephavenChannel;

import java.util.concurrent.CompletableFuture;

public interface SessionFactory {

    Session newSession();

    CompletableFuture<? extends Session> newSessionFuture();

    DeephavenChannel channel();
}
