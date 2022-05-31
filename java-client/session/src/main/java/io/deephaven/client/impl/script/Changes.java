package io.deephaven.client.impl.script;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.client.impl.FieldChanges;
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

    public abstract FieldChanges changes();

    public final boolean isEmpty() {
        return !errorMessage().isPresent() && changes().isEmpty();
    }

    public interface Builder {
        Builder errorMessage(String errorMessage);

        Builder changes(FieldChanges changes);

        Changes build();
    }
}
