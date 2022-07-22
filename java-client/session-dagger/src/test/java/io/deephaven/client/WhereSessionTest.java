package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereSessionTest extends DeephavenSessionTestBase {

    public static int myFunction() {
        return 42;
    }

    @Test
    public void allowStaticI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "i % 2 == 0");
    }

    @Test
    public void allowStaticII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TableSpec.empty(1), "ii % 2 == 0");
    }

    @Test
    public void allowTimeTableI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "i % 2 == 0");
    }

    @Test
    public void allowTimeTableII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)), "ii % 2 == 0");
    }

    @Test
    public void allowTickingI() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "i % 2 == 0");
    }

    @Test
    public void allowTickingII() throws InterruptedException, TableHandle.TableHandleException {
        allow(TimeTable.of(Duration.ofSeconds(1)).tail(1), "ii % 2 == 0");
    }

    @Test
    public void disallowCustomFunctions() throws InterruptedException {
        disallow(TableSpec.empty(1), String.format("%s.myFunction() == 42", WhereSessionTest.class.getName()));
    }

    @Test
    public void disallowNew() throws InterruptedException {
        disallow(TableSpec.empty(1), "new Object() == 42");
    }

    private void allow(TableSpec parent, String filter) throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle = session.batch().execute(parent.where(filter))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec parent, String filter) throws InterruptedException {
        try (final TableHandle handle = session.batch().execute(parent.where(filter))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

