package io.deephaven.client;

import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Feature;
import io.deephaven.qst.table.TableSpec;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SnapshotHistorySessionTest extends SnapshotWhenSessionTestBase {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        // snapshot history doesn't support all the specs yet
        return iterable(snapshotTicking(SnapshotWhenOptions.builder().addFlags(Feature.HISTORY)));
    }

    public SnapshotHistorySessionTest(TableSpec table) {
        super(table);
    }
}
