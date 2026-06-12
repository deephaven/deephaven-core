//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.web.client.state.ClientTableState;
import io.grpc.stub.StreamObserver;

/**
 * Describe how to perform initial fetch for a table
 */
public interface JsTableFetch {
    void fetch(StreamObserver<ExportedTableCreationResponse> callback, ClientTableState newState);
}
