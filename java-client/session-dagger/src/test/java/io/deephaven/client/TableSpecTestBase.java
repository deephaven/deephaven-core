package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableService;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class TableSpecTestBase extends DeephavenSessionTestBase {

    static Iterable<Object[]> iterable(TableSpec... specs) {
        return () -> Stream.of(specs).map(TableSpecTestBase::args).iterator();
    }

    private static Object[] args(TableSpec s) {
        return new Object[] {s};
    }

    private final TableSpec table;

    public TableSpecTestBase(TableSpec table) {
        this.table = Objects.requireNonNull(table);
    }

    @Test(timeout = 10000)
    public void batch() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.batch().execute(table)) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    @Test(timeout = 10000)
    public void serial() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.serial().execute(table)) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    @Test(timeout = 10000)
    public void async() throws ExecutionException, InterruptedException {
        try (final TableHandle handle = session.executeAsync(table).getOrCancel()) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }
}
