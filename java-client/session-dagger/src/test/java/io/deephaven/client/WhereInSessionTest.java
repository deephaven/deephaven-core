package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class WhereInSessionTest extends DeephavenSessionTestBase {

    @Test
    public void allowStatic1() throws InterruptedException, TableHandle.TableHandleException {
        allow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Foo=1", "Bar=2"), "Foo");
    }

    @Test
    public void allowStatic2() throws InterruptedException, TableHandle.TableHandleException {
        allow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Foo=1", "Bar=2"), "Foo", "Bar");
    }

    @Test
    public void allowStatic3() throws InterruptedException, TableHandle.TableHandleException {
        allow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Zip=1"), "Foo=Zip");
    }

    @Test
    public void allowTicking() throws InterruptedException, TableHandle.TableHandleException {
        allow(
                TimeTable.of(Duration.ofMillis(1)),
                TimeTable.of(Duration.ofMillis(2)), "Timestamp");
    }

    @Test
    public void disallowBadMatchName() throws InterruptedException {
        disallow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Foo=1", "Bar=2"), "Baz");
    }

    @Test
    public void disallowBadMatchName2() throws InterruptedException {
        disallow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Foo=1", "Bar=2"), "NonExistent");
    }

    @Test
    public void disallowMatchDirectionWrong() throws InterruptedException {
        disallow(
                TableSpec.empty(1).view("Foo=1", "Bar=2", "Baz=3"),
                TableSpec.empty(1).view("Zip=1"), "Zip=Foo");
    }

    private void allow(TableSpec left, TableSpec right, String... columnsToMatch)
            throws InterruptedException, TableHandle.TableHandleException {
        try (final TableHandle handle = session.batch().execute(left.whereIn(right, columnsToMatch))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
        try (final TableHandle handle = session.batch().execute(left.whereNotIn(right, columnsToMatch))) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private void disallow(TableSpec left, TableSpec right, String... columnsToMatch) throws InterruptedException {
        try (final TableHandle handle = session.serial().execute(left.whereIn(right, columnsToMatch))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
        try (final TableHandle handle = session.serial().execute(left.whereNotIn(right, columnsToMatch))) {
            failBecauseExceptionWasNotThrown(TableHandle.TableHandleException.class);
        } catch (TableHandle.TableHandleException e) {
            // expected
        }
    }
}

