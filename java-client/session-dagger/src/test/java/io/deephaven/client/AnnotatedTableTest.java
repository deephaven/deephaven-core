package io.deephaven.client;

import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.TableCreatorImpl;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class AnnotatedTableTest extends DeephavenSessionTestBase {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> annotatedTables() {
        return () -> AnnotatedTables.annotatedTables().entrySet().stream()
                .map(e -> new Object[] {e.getKey(), e.getValue()}).iterator();
    }

    private final AnnotatedTable table;

    public AnnotatedTableTest(@SuppressWarnings("unused") String name, AnnotatedTable table) {
        this.table = Objects.requireNonNull(table);
    }

    @Test
    public void executeSerial() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.serial().executeLogic(table.logic())) {
            checkResponse(handle);
        }
    }

    @Test
    public void executeBatch() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.batch(false).executeLogic(table.logic())) {
            checkResponse(handle);
        }
    }

    @Test
    public void executeSerialUnchecked() {
        // No real use in closing handle, we are leaking other tables. They should be cleaned up during Session close.
        final TableHandle handle = table.logic().create(session);
        checkResponse(handle);
    }

    @Test
    public void executeBatchExportAll() throws TableHandleException, InterruptedException {
        // Turn the logic into a spec
        final TableSpec tableSpec = table.logic().create(TableCreatorImpl.INSTANCE);

        // Get all specs
        final Set<TableSpec> allTables = ParentsVisitor.reachable(Collections.singleton(tableSpec));

        // Export all during the batch
        final List<TableHandle> handles = session.batch(false).execute(allTables);
        for (TableHandle handle : handles) {
            if (tableSpec.equals(handle.table())) {
                checkResponse(handle);
            }
            handle.close();
        }
    }

    private void checkResponse(TableHandle handle) {
        final ExportedTableCreationResponse response = handle.response();
        if (table.isStatic()) {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isTrue();
            assertThat(response.getSize()).isEqualTo(table.size());
        } else {
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getIsStatic()).isFalse();
        }
    }
}
