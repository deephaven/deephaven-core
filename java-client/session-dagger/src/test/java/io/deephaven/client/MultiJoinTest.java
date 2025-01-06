//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.qst.table.MultiJoinInput;
import io.deephaven.qst.table.MultiJoinTable;
import io.deephaven.qst.table.TableSpec;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiJoinTest extends DeephavenSessionTestBase {

    @Test
    public void multiJoinTableExecute() throws TableHandleException, InterruptedException {
        try (final TableHandle handle = session.batch().execute(prototype())) {
            assertThat(handle.isSuccessful()).isTrue();
        }
    }

    private MultiJoinTable prototype() {
        final Table t1 = TableTools.newTable(
                TableTools.longCol("Key", 0L),
                TableTools.longCol("First", 0L));
        final Table t2 = TableTools.newTable(
                TableTools.longCol("Key", 0L),
                TableTools.longCol("Second", 1L));
        final Table t3 = TableTools.newTable(
                TableTools.longCol("Key", 0L),
                TableTools.longCol("Third", 2L));
        return MultiJoinTable.builder()
                .addInputs(MultiJoinInput.<TableSpec>builder()
                        .table(ref(t1))
                        .addMatches(JoinMatch.parse("OutputKey=Key"))
                        .addAdditions(ColumnName.of("First"))
                        .build())
                .addInputs(MultiJoinInput.<TableSpec>builder()
                        .table(ref(t2))
                        .addMatches(JoinMatch.parse("OutputKey=Key"))
                        .addAdditions(ColumnName.of("Second"))
                        .build())
                .addInputs(MultiJoinInput.<TableSpec>builder()
                        .table(ref(t3))
                        .addMatches(JoinMatch.parse("OutputKey=Key"))
                        .addAdditions(ColumnName.of("Third"))
                        .build())
                .build();

    }
}
