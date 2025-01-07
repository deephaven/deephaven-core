//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.fu.JsBiConsumer;

/**
 * Describe how to perform initial fetch for a table
 */
public interface JsTableFetch {
    void fetch(JsBiConsumer<Object, ExportedTableCreationResponse> callback, ClientTableState newState,
            BrowserHeaders metadata);
}
