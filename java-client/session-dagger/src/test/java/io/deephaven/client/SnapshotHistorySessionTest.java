//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Builder;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.qst.table.TableSpec;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SnapshotHistorySessionTest extends SnapshotWhenSessionTestBase {

    private static Builder getBuilder() {
        return SnapshotWhenOptions.builder().addFlags(Flag.HISTORY);
    }

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        // snapshot history doesn't support all the specs yet
        return iterable(
                snapshotTicking(getBuilder()),
                snapshotTickingStamp(getBuilder()),
                snapshotTickingStampRename(getBuilder()));
    }

    public SnapshotHistorySessionTest(TableSpec table) {
        super(table);
    }
}
