package io.deephaven.client.examples.tutorials;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.column.header.ColumnHeader;

public enum Miami implements TableCreationLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        // @formatter:off
        return creation.of(ColumnHeader.ofString("Month")
                .header(ColumnHeader.ofInt("Temp"))
                .header(ColumnHeader.ofDouble("Rain"))
                .row("Jan", 60, 1.62)
                .row("Feb", 62, 2.25)
                .row("Mar", 65, 3.00)
                .row("Apr", 68, 3.14)
                .row("May", 73, 5.34)
                .row("Jun", 76, 9.67)
                .row("Jul", 77, 6.50)
                .row("Aug", 77, 8.88)
                .row("Sep", 76, 9.86)
                .row("Oct", 74, 6.33)
                .row("Nov", 68, 3.27)
                .row("Dec", 63, 2.04)
                .newTable());
        // @formatter:on
    }
}
