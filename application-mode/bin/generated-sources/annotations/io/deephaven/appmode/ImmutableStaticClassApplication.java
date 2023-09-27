package io.deephaven.appmode;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Booleans;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link StaticClassApplication}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableStaticClassApplication.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableStaticClassApplication.of()}.
 */
@Generated(from = "StaticClassApplication", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableStaticClassApplication<T extends Application.Factory>
    extends StaticClassApplication<T> {
  private final Class<T> clazz;
  private final boolean isEnabled;

  private ImmutableStaticClassApplication(Class<T> clazz, boolean isEnabled) {
    this.clazz = Objects.requireNonNull(clazz, "clazz");
    this.isEnabled = isEnabled;
  }

  private ImmutableStaticClassApplication(ImmutableStaticClassApplication<T> original, Class<T> clazz, boolean isEnabled) {
    this.clazz = clazz;
    this.isEnabled = isEnabled;
  }

  /**
   * @return The value of the {@code clazz} attribute
   */
  @Override
  public Class<T> clazz() {
    return clazz;
  }

  /**
   * @return The value of the {@code isEnabled} attribute
   */
  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link StaticClassApplication#clazz() clazz} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for clazz
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableStaticClassApplication<T> withClazz(Class<T> value) {
    if (this.clazz == value) return this;
    Class<T> newValue = Objects.requireNonNull(value, "clazz");
    return validate(new ImmutableStaticClassApplication<>(this, newValue, this.isEnabled));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link StaticClassApplication#isEnabled() isEnabled} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for isEnabled
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableStaticClassApplication<T> withIsEnabled(boolean value) {
    if (this.isEnabled == value) return this;
    return validate(new ImmutableStaticClassApplication<>(this, this.clazz, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableStaticClassApplication} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableStaticClassApplication<?>
        && equalTo(0, (ImmutableStaticClassApplication<?>) another);
  }

  private boolean equalTo(int synthetic, ImmutableStaticClassApplication<?> another) {
    return clazz.equals(another.clazz)
        && isEnabled == another.isEnabled;
  }

  /**
   * Computes a hash code from attributes: {@code clazz}, {@code isEnabled}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + clazz.hashCode();
    h += (h << 5) + Booleans.hashCode(isEnabled);
    return h;
  }

  /**
   * Prints the immutable value {@code StaticClassApplication} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("StaticClassApplication")
        .omitNullValues()
        .add("clazz", clazz)
        .add("isEnabled", isEnabled)
        .toString();
  }

  /**
   * Construct a new immutable {@code StaticClassApplication} instance.
 * @param <T> generic parameter T
   * @param clazz The value for the {@code clazz} attribute
   * @param isEnabled The value for the {@code isEnabled} attribute
   * @return An immutable StaticClassApplication instance
   */
  public static <T extends Application.Factory> ImmutableStaticClassApplication<T> of(Class<T> clazz, boolean isEnabled) {
    return validate(new ImmutableStaticClassApplication<>(clazz, isEnabled));
  }

  private static <T extends Application.Factory> ImmutableStaticClassApplication<T> validate(ImmutableStaticClassApplication<T> instance) {
    instance.checkClazz();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link StaticClassApplication} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param <T> generic parameter T
   * @param instance The instance to copy
   * @return A copied immutable StaticClassApplication instance
   */
  public static <T extends Application.Factory> ImmutableStaticClassApplication<T> copyOf(StaticClassApplication<T> instance) {
    if (instance instanceof ImmutableStaticClassApplication<?>) {
      return (ImmutableStaticClassApplication<T>) instance;
    }
    return ImmutableStaticClassApplication.<T>builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableStaticClassApplication ImmutableStaticClassApplication}.
   * <pre>
   * ImmutableStaticClassApplication.&amp;lt;T&amp;gt;builder()
   *    .clazz(Class&amp;lt;T&amp;gt;) // required {@link StaticClassApplication#clazz() clazz}
   *    .isEnabled(boolean) // required {@link StaticClassApplication#isEnabled() isEnabled}
   *    .build();
   * </pre>
   * @param <T> generic parameter T
   * @return A new ImmutableStaticClassApplication builder
   */
  public static <T extends Application.Factory> ImmutableStaticClassApplication.Builder<T> builder() {
    return new ImmutableStaticClassApplication.Builder<>();
  }

  /**
   * Builds instances of type {@link ImmutableStaticClassApplication ImmutableStaticClassApplication}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "StaticClassApplication", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder<T extends Application.Factory> {
    private static final long INIT_BIT_CLAZZ = 0x1L;
    private static final long INIT_BIT_IS_ENABLED = 0x2L;
    private long initBits = 0x3L;

    private @Nullable Class<T> clazz;
    private boolean isEnabled;

    private Builder() {
    }

    /**
<<<<<<< HEAD
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.StaticClassApplication} instance.
=======
     * Fill a builder with attribute values from the provided {@code StaticClassApplication} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
>>>>>>> main
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> from(StaticClassApplication<T> instance) {
      Objects.requireNonNull(instance, "instance");
<<<<<<< HEAD
      from((Object) instance);
=======
      clazz(instance.clazz());
      isEnabled(instance.isEnabled());
>>>>>>> main
      return this;
    }

    /**
<<<<<<< HEAD
     * Fill a builder with attribute values from the provided {@code io.deephaven.appmode.ApplicationConfig} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> from(ApplicationConfig instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    @SuppressWarnings("unchecked")
    private void from(Object object) {
      if (object instanceof StaticClassApplication<?>) {
        StaticClassApplication<T> instance = (StaticClassApplication<T>) object;
        clazz(instance.clazz());
      }
      if (object instanceof ApplicationConfig) {
        ApplicationConfig instance = (ApplicationConfig) object;
        isEnabled(instance.isEnabled());
      }
    }

    /**
=======
>>>>>>> main
     * Initializes the value for the {@link StaticClassApplication#clazz() clazz} attribute.
     * @param clazz The value for clazz 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> clazz(Class<T> clazz) {
      this.clazz = Objects.requireNonNull(clazz, "clazz");
      initBits &= ~INIT_BIT_CLAZZ;
      return this;
    }

    /**
     * Initializes the value for the {@link StaticClassApplication#isEnabled() isEnabled} attribute.
     * @param isEnabled The value for isEnabled 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder<T> isEnabled(boolean isEnabled) {
      this.isEnabled = isEnabled;
      initBits &= ~INIT_BIT_IS_ENABLED;
      return this;
    }

    /**
     * Builds a new {@link ImmutableStaticClassApplication ImmutableStaticClassApplication}.
     * @return An immutable instance of StaticClassApplication
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableStaticClassApplication<T> build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableStaticClassApplication.validate(new ImmutableStaticClassApplication<>(null, clazz, isEnabled));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_CLAZZ) != 0) attributes.add("clazz");
      if ((initBits & INIT_BIT_IS_ENABLED) != 0) attributes.add("isEnabled");
      return "Cannot build StaticClassApplication, some of required attributes are not set " + attributes;
    }
  }
}
