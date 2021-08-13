package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreationLabeledLogic;

public enum ExampleFunctionsMore implements TableCreationLabeledLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> LabeledValues<T> create(TableCreator<T> creation) {
        LabeledValues<T> otherValues = ExampleFunctions.INSTANCE.create(creation);
        return LabeledValues.<T>builder().addPrefixed("example-1/", otherValues).build();
    }
}
