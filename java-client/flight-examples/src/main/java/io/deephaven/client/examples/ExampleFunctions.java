package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreator;

import java.util.Arrays;

public enum ExampleFunctions implements TableCreationLabeledLogic {
    INSTANCE;

    @Override
    public <T extends TableOperations<T, T>> LabeledValues<T> create(TableCreator<T> creation) {
        T example1 = ExampleFunction1.INSTANCE.create(creation);
        T example2 = ExampleFunction2.INSTANCE.create(creation);
        T example3 = ExampleFunction3.INSTANCE.create(creation);
        T out = creation.merge(Arrays.asList(example1, example2, example3));
        for (int i = 0; i < 100; ++i) {
            out = creation.merge(Arrays.asList(out.head(1), out.tail(1)));
        }
        return LabeledValues.<T>builder().add(ExampleFunction1.class.getName(), example1)
            .add(ExampleFunction2.class.getName(), example2)
            .add(ExampleFunction3.class.getName(), example3).add("merged", out).build();
    }
}
