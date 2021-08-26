/*
 * Copyright (c) 2016-2021. Deephaven Data Labs and Patent Pending.
 */

package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;

/**
 * A listener to replay the current table snapshot.
 */
@ScriptApi
public interface TableSnapshotReplayer {

    /**
     * Replay the current table snapshot into a listener. A shared or exclusive LTM lock should be held when calling
     * this method.
     */
    void replay();
}
