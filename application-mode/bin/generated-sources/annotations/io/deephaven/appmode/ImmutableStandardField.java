package io.deephaven.appmode;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link StandardField}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableStandardField.builder()}.
 */
@Generated(from = "StandardField", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableStandardField<T> extends StandardField<T> {
  private final String name;
  private final T value;
  private final @Nullable String description;

  private ImmutableStandardField(String name, T value, @Nullable String description) {
    this.name = name;
    this.value = value;
    this.description = description;
  }

  /**
   * @return The value of the {@code name} attribute
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public T value() {
    return value;
  }

  /**
   * @return The value of the {@code description} attribute
   */
  @Override
  public Optional<String> description() {
    return Optional.ofNullable(description);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link StandardField#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableStandardField<T> withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableStandardField<>(newValue, this.value, this.description));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link StandardField#value() value} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableStandardField<T> withValue(T value) {
    if (this.value == value) return this;
    T newValue = Objects.requireNonNull(value, "value");
    return validate(new ImmutableStandardField<>(this.name, newValue, this.description));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link StandardField#description() description} attribute.
   * @param value The value for description
   * @return A modified copy of {@code this} object
   */
  public final ImmutableStandardField<T> withDescription(String value) {
    @Nullable String newValue = Objects.requireNonNull(value, "description");
    if (Objects.equals(this.description, newValue)) return this;
    return validate(new ImmutableStandardField<>(this.name, this.value, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link StandardField#description() description} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for description
   * @return A modified copy of {@code this} object
   */
  public final ImmutableStandardField<T> withDescription(Optional<String> optional) {
    @Nullable String value = optional.orElse(null);
    if (Objects.equals(this.description, value)) return this;
    return validate(new ImmutableStandardField<>(this.name, this.value, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableStandardField} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableStandardField<?>
        && equalTo(0, (ImmutableStandardField<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableStandardField<?> another) {
    return name.equals(another.name)
        && value.equals(another.value)
        && Objects.equals(description, another.description);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code value}, {@code description}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + value.hashCode();
    h += (h << 5) + Objects.hashCode(description);
    return h;
  }

  /**
   * Prints the immutable value {@code StandardField} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("StandardField")
        .omitNullValues()
        .add("name", name)
        .add("value", value)
        .add("description", description)
        .toString();
  }

  private static <T> ImmutableStandardField<T> validate(ImmutableStandardField<T> instance) {
    instance.checkDescription();
    instance.checkName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link StandardField} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable StandardField instance
   */
  public static <T> ImmutableStandardField<T> copyOf(StandardField<T> instance) {
    if (instance instanceof ImmutableStandardField<?>) {
      return (ImmutableStandardField<T>) instance;
    }
    return ImmutableStandardField.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableStandardField ImmutableStandardField}.
   * <pre>
   * ImmutableStandardField.&amp;lt;T&amp;gt;builder()
   *    .name(String) // required {@link StandardField#name() name}
   *    .value(T) // required {@link StandardField#value() value}
   *    .description(String) // optional {@link StandardField#description() description}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableStandardField builder
   */
  public static <T> ImmutableStandardField.Builder<T> builder() {
    return new ImmutableStandardField.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableStandardField ImmutableStandardField}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "StandardField", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder<T> {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_VALUE = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String name;
    private @Nullable T value;
    private @Nullable String description;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.Field} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> from(Field<T> instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.StandardField} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> from(StandardField<T> instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    @SuppressWarnings("unchecked")
    private void from(Object object) {
      if (object instanceof Field<?>) {
        Field<T> instance = (Field<T>) object;
        name(instance.name());
        Optional<String> descriptionOptional = instance.description();
        if (descriptionOptional.isPresent()) {
          description(descriptionOptional);
        }
        value(instance.value());
      }
    }

    /**
     * Initializes the value for the {@link StandardField#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link StandardField#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> value(T value) {
      this.value = Objects.requireNonNull(value, "value");
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Initializes the optional value {@link StandardField#description() description} to description.
     * @param description The value for description
     * @return {@code this} builder for chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> description(String description) {
      this.description = Objects.requireNonNull(description, "description");
      return this;
    }

    /**
     * Initializes the optional value {@link StandardField#description() description} to description.
     * @param description The value for description
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> description(Optional<String> description) {
      this.description = description.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableStandardField ImmutableStandardField}.
     * @return An immutable instance of StandardField
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableStandardField<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableStandardField.validate(new ImmutableStandardField<>(name, value, description));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build StandardField, some of required attributes are not set " + attributes;
    }
  }
}
