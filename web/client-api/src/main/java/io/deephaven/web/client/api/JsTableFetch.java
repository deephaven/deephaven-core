//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.web.client.state.ClientTableState;
import io.grpc.stub.StreamObserver;

/**
 * Describe how to perform initial fetch for a table.
 * <p>
 * The fetcher runs in a cancellable gRPC context of its own, which canceling the fetch cancels: calls it makes before
 * returning are ended with it, or not made. A fetcher that makes its call later, after a promise, must capture
 * {@code Context.current()} when it runs and make the call in it. Work it starts that is not its own, such as a shared
 * fetch or a reconnect, must not run in that context.
 */
public interface JsTableFetch {
    void fetch(StreamObserver<ExportedTableCreationResponse> callback, ClientTableState newState);
}
