package io.deephaven.client.examples.tutorials;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.column.header.ColumnHeader;

public enum CreateANewTable implements TableCreationLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        // @formatter:off
        return creation.of(ColumnHeader.ofString("NameOfStringCol")
                .header(ColumnHeader.ofInt("NameOfIntCol"))
                .row("Data String 1", 4)
                .row("Data String 2", 5)
                .row("Data String 3", 6)
                .newTable());
        // @formatter:on
    }
}
