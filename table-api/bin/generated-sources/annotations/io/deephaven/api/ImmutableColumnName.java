package io.deephaven.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ColumnName}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableColumnName.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableColumnName.of()}.
 */
@Generated(from = "ColumnName", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableColumnName extends ColumnName {
  private final String name;

  private ImmutableColumnName(String name) {
    this.name = Objects.requireNonNull(name, "name");
  }

  private ImmutableColumnName(ImmutableColumnName original, String name) {
    this.name = name;
  }

  /**
   * The column name.
   * @return the column name
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ColumnName#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableColumnName withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableColumnName(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableColumnName} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableColumnName
        && equalTo(0, (ImmutableColumnName) another);
  }

  private boolean equalTo(int synthetic, ImmutableColumnName another) {
    return name.equals(another.name);
  }

  /**
   * Computes a hash code from attributes: {@code name}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code ColumnName} instance.
   * @param name The value for the {@code name} attribute
   * @return An immutable ColumnName instance
   */
  public static ImmutableColumnName of(String name) {
    return validate(new ImmutableColumnName(name));
  }

  private static ImmutableColumnName validate(ImmutableColumnName instance) {
    instance.checkName();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ColumnName} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ColumnName instance
   */
  public static ImmutableColumnName copyOf(ColumnName instance) {
    if (instance instanceof ImmutableColumnName) {
      return (ImmutableColumnName) instance;
    }
    return ImmutableColumnName.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableColumnName ImmutableColumnName}.
   * <pre>
   * ImmutableColumnName.builder()
   *    .name(String) // required {@link ColumnName#name() name}
   *    .build();
   * </pre>
   * @return A new ImmutableColumnName builder
   */
  public static ImmutableColumnName.Builder builder() {
    return new ImmutableColumnName.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableColumnName ImmutableColumnName}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ColumnName", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_NAME = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String name;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ColumnName} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ColumnName instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      return this;
    }

    /**
     * Initializes the value for the {@link ColumnName#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Builds a new {@link ImmutableColumnName ImmutableColumnName}.
     * @return An immutable instance of ColumnName
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableColumnName build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableColumnName.validate(new ImmutableColumnName(null, name));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      return "Cannot build ColumnName, some of required attributes are not set " + attributes;
    }
  }
}
