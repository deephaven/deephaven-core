package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLabeledLogic;

public class TutorialTables implements TableCreationLabeledLogic {
    @Override
    public <T extends TableOperations<T, T>> LabeledValues<T> create(TableCreator<T> creation) {


        return LabeledValues.<T>builder()
            .add("https://deephaven.io/core/docs/tutorials/new-table", null).build();
    }
}
