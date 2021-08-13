package io.deephaven.client.examples.tutorials;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLabeledLogic;

public enum Tutorials implements TableCreationLabeledLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> LabeledValues<T> create(TableCreator<T> creation) {
        return LabeledValues.<T>builder()
            // .add(CreateANewTable.class.getName(), CreateANewTable.INSTANCE.create(creation))
            .add(CreateATimeTable.class.getName(), CreateATimeTable.INSTANCE.create(creation))
            // .add(Miami.class.getName(), Miami.INSTANCE.create(creation))
            // .add(Grades.class.getName(), Grades.INSTANCE.create(creation))
            .build();
    }
}
