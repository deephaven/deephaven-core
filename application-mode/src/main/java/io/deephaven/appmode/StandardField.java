package io.deephaven.appmode;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import javax.lang.model.SourceVersion;
import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class StandardField<T> implements Field<T> {

    public static <T> Field<T> of(String name, T value) {
        return ImmutableStandardField.<T>builder().name(name).value(value).build();
    }

    public static <T> Field<T> of(String name, T value, String description) {
        return ImmutableStandardField.<T>builder().name(name).value(value).description(description)
                .build();
    }

    public abstract String name();

    public abstract T value();

    public abstract Optional<String> description();

    @Check
    final void checkName() {
        if (!SourceVersion.isName(name())) {
            throw new IllegalArgumentException(
                    "name() is invalid, must conform to javax.lang.model.SourceVersion#isName");
        }
    }

    @Check
    final void checkDescription() {
        if (description().isPresent() && description().get().isEmpty()) {
            throw new IllegalArgumentException("description(), when present, must not be empty");
        }
    }
}
