package io.deephaven.client;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;

@RunWith(Parameterized.class)
public class SnapshotSessionTest extends TableSpecTestBase {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> specs() {
        return iterable(snapshot(), snapshotTicking());
    }

    static TableSpec snapshot() {
        return EmptyTable.of(1000).view("I=i").snapshot();
    }

    static TableSpec snapshotTicking() {
        return TimeTable.of(Duration.ofSeconds(1)).snapshot();
    }

    public SnapshotSessionTest(TableSpec table) {
        super(table);
    }
}
