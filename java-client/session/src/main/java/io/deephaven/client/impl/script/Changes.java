package io.deephaven.client.impl.script;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class Changes {

    public static Builder builder() {
        return ImmutableChanges.builder();
    }

    public abstract Optional<String> errorMessage();

    public abstract List<VariableDefinition> created();

    public abstract List<VariableDefinition> updated();

    public abstract List<VariableDefinition> removed();

    public final boolean isEmpty() {
        return created().isEmpty() && updated().isEmpty() && removed().isEmpty();
    }

    public interface Builder {
        Builder errorMessage(String errorMessage);

        Builder addCreated(VariableDefinition element);

        Builder addCreated(VariableDefinition... elements);

        Builder addAllCreated(Iterable<? extends VariableDefinition> elements);

        Builder addUpdated(VariableDefinition element);

        Builder addUpdated(VariableDefinition... elements);

        Builder addAllUpdated(Iterable<? extends VariableDefinition> elements);

        Builder addRemoved(VariableDefinition element);

        Builder addRemoved(VariableDefinition... elements);

        Builder addAllRemoved(Iterable<? extends VariableDefinition> elements);

        Changes build();
    }
}
