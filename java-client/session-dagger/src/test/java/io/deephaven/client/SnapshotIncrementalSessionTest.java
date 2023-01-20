package io.deephaven.client;

import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.qst.table.TableSpec;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SnapshotIncrementalSessionTest extends SnapshotWhenSessionTestBase {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        return specs(() -> SnapshotWhenOptions.builder().addFlags(Flag.INCREMENTAL));
    }

    public SnapshotIncrementalSessionTest(TableSpec table) {
        super(table);
    }
}
