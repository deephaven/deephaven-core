package io.deephaven.appmode;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class CustomField<T> implements Field<T> {

    public static <T> Builder<T> builder(String type) {
        return ImmutableCustomField.<T>builder().type(type);
    }

    public abstract String name();

    public abstract T value();

    public abstract String type();

    public abstract Optional<String> description();

    @Check
    final void checkName() {
        if (!ApplicationUtil.isAsciiPrintable(name())) {
            throw new IllegalArgumentException("name() is invalid, must be printable ascii");
        }
    }

    @Check
    final void checkType() {
        if (!ApplicationUtil.isAsciiPrintable(name())) {
            throw new IllegalArgumentException("name() is invalid, must be printable ascii");
        }
    }

    @Check
    final void checkDescription() {
        if (description().isPresent() && description().get().isEmpty()) {
            throw new IllegalArgumentException("description(), when present, must not be empty");
        }
    }

    public interface Builder<T> {
        default CustomField<T> of(String name, T value) {
            return name(name).value(value).build();
        }

        default CustomField<T> of(String name, T value, String description) {
            return name(name).value(value).description(description).build();
        }

        Builder<T> name(String name);

        Builder<T> value(T value);

        Builder<T> description(String description);

        CustomField<T> build();
    }
}
