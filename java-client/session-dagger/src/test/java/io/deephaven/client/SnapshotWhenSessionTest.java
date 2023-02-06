package io.deephaven.client;

import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.qst.table.TableSpec;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SnapshotWhenSessionTest extends SnapshotWhenSessionTestBase {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        return specs(SnapshotWhenOptions::builder);
    }

    public SnapshotWhenSessionTest(TableSpec table) {
        super(table);
    }
}
