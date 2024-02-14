/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * A server object is a client-owned reference to a server-side object.
 */
public interface ServerObject extends HasExportId, Closeable {

    /**
     * Releases {@code this}. After releasing, callers should not use {@code this} object nor {@link #exportId()} for
     * additional RPC calls.
     *
     * @return the future
     * @see Session#release(ExportId)
     */
    CompletableFuture<Void> release();

    /**
     * Releases {@code this} without waiting for the result. After closing, callers should not use {@code this} object
     * nor {@link #exportId()} for additional RPC calls. For more control, see {@link #release()}.
     */
    @Override
    void close();
}
