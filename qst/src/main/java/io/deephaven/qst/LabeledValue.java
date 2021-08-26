package io.deephaven.qst;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class LabeledValue<T> {

    public static <T> LabeledValue<T> of(String name, T value) {
        return ImmutableLabeledValue.of(name, value);
    }

    @Parameter
    public abstract String name();

    @Parameter
    public abstract T value();
}
