package io.deephaven.qst.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CustomType}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCustomType.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableCustomType.of()}.
 */
@Generated(from = "CustomType", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableCustomType<T> extends CustomType<T> {
  private final Class<T> clazz;

  private ImmutableCustomType(Class<T> clazz) {
    this.clazz = Objects.requireNonNull(clazz, "clazz");
  }

  private ImmutableCustomType(ImmutableCustomType<T> original, Class<T> clazz) {
    this.clazz = clazz;
  }

  /**
   * @return The value of the {@code clazz} attribute
   */
  @Override
  public Class<T> clazz() {
    return clazz;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CustomType#clazz() clazz} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clazz
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCustomType<T> withClazz(Class<T> value) {
    if (this.clazz == value) return this;
    Class<T> newValue = Objects.requireNonNull(value, "clazz");
    return validate(new ImmutableCustomType<>(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCustomType} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCustomType<?>
        && equalTo(0, (ImmutableCustomType<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableCustomType<?> another) {
    return clazz.equals(another.clazz);
  }

  /**
   * Computes a hash code from attributes: {@code clazz}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + clazz.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code CustomType} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CustomType{"
        + "clazz=" + clazz
        + "}";
  }

  /**
   * Construct a new immutable {@code CustomType} instance.
 * @param <T> generic parameter T
   * @param clazz The value for the {@code clazz} attribute
   * @return An immutable CustomType instance
   */
  public static <T> ImmutableCustomType<T> of(Class<T> clazz) {
    return validate(new ImmutableCustomType<>(clazz));
  }

  private static <T> ImmutableCustomType<T> validate(ImmutableCustomType<T> instance) {
    instance.checkNotVector();
    instance.checkNotArray();
    instance.checkNotStatic();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link CustomType} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable CustomType instance
   */
  public static <T> ImmutableCustomType<T> copyOf(CustomType<T> instance) {
    if (instance instanceof ImmutableCustomType<?>) {
      return (ImmutableCustomType<T>) instance;
    }
    return ImmutableCustomType.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCustomType ImmutableCustomType}.
   * <pre>
   * ImmutableCustomType.&amp;lt;T&amp;gt;builder()
   *    .clazz(Class&amp;lt;T&amp;gt;) // required {@link CustomType#clazz() clazz}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableCustomType builder
   */
  public static <T> ImmutableCustomType.Builder<T> builder() {
    return new ImmutableCustomType.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableCustomType ImmutableCustomType}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CustomType", generator = "Immutables")
  public static final class Builder<T> {
    private static final long INIT_BIT_CLAZZ = 0x1L;
    private long initBits = 0x1L;

    private Class<T> clazz;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.type.CustomType} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(CustomType<T> instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.type.GenericTypeBase} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> from(GenericTypeBase<T> instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    @SuppressWarnings("unchecked")
    private void from(Object object) {
      long bits = 0;
      if (object instanceof CustomType<?>) {
        CustomType<T> instance = (CustomType<T>) object;
        if ((bits & 0x1L) == 0) {
          clazz(instance.clazz());
          bits |= 0x1L;
        }
      }
      if (object instanceof GenericTypeBase<?>) {
        GenericTypeBase<T> instance = (GenericTypeBase<T>) object;
        if ((bits & 0x1L) == 0) {
          clazz(instance.clazz());
          bits |= 0x1L;
        }
      }
    }

    /**
     * Initializes the value for the {@link CustomType#clazz() clazz} attribute.
     * @param clazz The value for clazz 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder<T> clazz(Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      initBits &= ~INIT_BIT_CLAZZ;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCustomType ImmutableCustomType}.
     * @return An immutable instance of CustomType
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCustomType<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableCustomType.validate(new ImmutableCustomType<>(null, clazz));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLAZZ) != 0) attributes.add("clazz");
      return "Cannot build CustomType, some of required attributes are not set " + attributes;
    }
  }
}
