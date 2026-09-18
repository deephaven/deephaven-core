//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import elemental2.core.ReadonlyArray;
import io.deephaven.web.client.api.JsTable;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByOperation {
    public UpdateBySpecUnion spec;
    public ReadonlyArray<JsTable.MatchPairUnion> matchPairs;
}
