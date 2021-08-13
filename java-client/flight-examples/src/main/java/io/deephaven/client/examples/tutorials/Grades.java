package io.deephaven.client.examples.tutorials;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.column.header.ColumnHeader;

public enum Grades implements TableCreationLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        // @formatter:off
        return creation.of(ColumnHeader.ofString("Name")
                .header(ColumnHeader.ofInt("Test1"))
                .header(ColumnHeader.ofInt("Test2"))
                .header(ColumnHeader.ofInt("Average"))
                .header(ColumnHeader.ofDouble("GPA"))
                .row("Ashley", 92, 94, 93, 3.9)
                .row("Jeff", 78,88, 83, 2.9)
                .row("Rita", 87, 81, 84, 3.0)
                .row("Zach", 74, 70, 72, 1.8)
                .newTable());
        // @formatter:on
    }
}
