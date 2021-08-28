package io.deephaven.client.impl.script;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class VariableDefinition {

    public static VariableDefinition of(String type, String title) {
        return ImmutableVariableDefinition.of(type, title);
    }

    @Parameter
    public abstract String type();

    @Parameter
    public abstract String title();
}
