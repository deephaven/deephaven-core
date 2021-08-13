package io.deephaven.client.examples.tutorials;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;

public enum CreateATimeTable implements TableCreationLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        return creation.of(TimeTable.of(Duration.ofSeconds(2)));
    }
}
