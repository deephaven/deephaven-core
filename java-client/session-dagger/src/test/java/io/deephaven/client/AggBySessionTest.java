package io.deephaven.client;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class AggBySessionTest extends DeephavenSessionTestBase {

    public static int myFunction() {
        return 42;
    }

    @Test
    public void allowStaticI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "Key = i % 2");
    }

    @Test
    public void allowStaticII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "Key = ii % 2");
    }

    @Test
    public void allowTimeTableI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "Key = i % 2");
    }

    @Test
    public void allowTimeTableII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "Key = ii % 2");
    }

    @Test
    public void allowTickingI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "Key = i % 2");
    }

    @Test
    public void allowTickingII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "Key = ii % 2");
    }

    @Test
    public void disallowCustomFunctions() throws InterruptedException {
        disallow(TableSpec.empty(1), String.format("Key = %s.myFunction()", AggBySessionTest.class.getName()));
    }

    @Test
    public void disallowNew() throws InterruptedException {
        disallow(TableSpec.empty(1), "Key = new Object()");
    }

    @Test
    public void allowPreviousColumn() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1), "X = 12", "Y = X + 1");
    }

    @Test
    public void disallowFutureColumn() throws InterruptedException {
        disallow(TableSpec.empty(1), "Y = X + 1", "X = 12");
    }

    @Test
    public void allowReassignmentColumn() throws TableHandle.TableHandleException, InterruptedException {
        allow(TableSpec.empty(1), "X = 12", "Y = X + 1", "X = 42");
    }

    @Test
    public void disallowNonExistentColumn() throws InterruptedException {
        disallow(TableSpec.empty(1), "X = 12", "Y = Z + 1");
    }

    private void allow(TableSpec parent, String... groupBys)
            throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle =
                session.batch().execute(parent.aggBy(Aggregation.AggCount("Count"), groupBys))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec parent, String... groupBys) throws InterruptedException {
        try (final TableHandle handle =
                session.batch().execute(parent.aggBy(Aggregation.AggCount("Count"), groupBys))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

