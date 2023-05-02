package io.deephaven.api.updateby.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link WindowScale}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableWindowScale.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableWindowScale.of()}.
 */
@Generated(from = "WindowScale", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableWindowScale extends WindowScale {
  private final @Nullable String timestampCol;
  private final long timescaleUnits;

  private ImmutableWindowScale(@Nullable String timestampCol, long timescaleUnits) {
    this.timestampCol = timestampCol;
    this.timescaleUnits = timescaleUnits;
  }

  /**
   * @return The value of the {@code timestampCol} attribute
   */
  @Override
  public @Nullable String timestampCol() {
    return timestampCol;
  }

  /**
   * @return The value of the {@code timescaleUnits} attribute
   */
  @Override
  public long timescaleUnits() {
    return timescaleUnits;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WindowScale#timestampCol() timestampCol} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timestampCol (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWindowScale withTimestampCol(@Nullable String value) {
    if (Objects.equals(this.timestampCol, value)) return this;
    return validate(new ImmutableWindowScale(value, this.timescaleUnits));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link WindowScale#timescaleUnits() timescaleUnits} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timescaleUnits
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableWindowScale withTimescaleUnits(long value) {
    if (this.timescaleUnits == value) return this;
    return validate(new ImmutableWindowScale(this.timestampCol, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableWindowScale} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableWindowScale
        && equalTo(0, (ImmutableWindowScale) another);
  }

  private boolean equalTo(int synthetic, ImmutableWindowScale another) {
    return Objects.equals(timestampCol, another.timestampCol)
        && timescaleUnits == another.timescaleUnits;
  }

  /**
   * Computes a hash code from attributes: {@code timestampCol}, {@code timescaleUnits}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(timestampCol);
    h += (h << 5) + Long.hashCode(timescaleUnits);
    return h;
  }

  /**
   * Prints the immutable value {@code WindowScale} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "WindowScale{"
        + "timestampCol=" + timestampCol
        + ", timescaleUnits=" + timescaleUnits
        + "}";
  }

  /**
   * Construct a new immutable {@code WindowScale} instance.
   * @param timestampCol The value for the {@code timestampCol} attribute
   * @param timescaleUnits The value for the {@code timescaleUnits} attribute
   * @return An immutable WindowScale instance
   */
  public static ImmutableWindowScale of(@Nullable String timestampCol, long timescaleUnits) {
    return validate(new ImmutableWindowScale(timestampCol, timescaleUnits));
  }

  private static ImmutableWindowScale validate(ImmutableWindowScale instance) {
    instance.checkTimestampColEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link WindowScale} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable WindowScale instance
   */
  public static ImmutableWindowScale copyOf(WindowScale instance) {
    if (instance instanceof ImmutableWindowScale) {
      return (ImmutableWindowScale) instance;
    }
    return ImmutableWindowScale.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableWindowScale ImmutableWindowScale}.
   * <pre>
   * ImmutableWindowScale.builder()
   *    .timestampCol(String | null) // nullable {@link WindowScale#timestampCol() timestampCol}
   *    .timescaleUnits(long) // required {@link WindowScale#timescaleUnits() timescaleUnits}
   *    .build();
   * </pre>
   * @return A new ImmutableWindowScale builder
   */
  public static ImmutableWindowScale.Builder builder() {
    return new ImmutableWindowScale.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableWindowScale ImmutableWindowScale}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "WindowScale", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TIMESCALE_UNITS = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String timestampCol;
    private long timescaleUnits;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code WindowScale} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(WindowScale instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable String timestampColValue = instance.timestampCol();
      if (timestampColValue != null) {
        timestampCol(timestampColValue);
      }
      timescaleUnits(instance.timescaleUnits());
      return this;
    }

    /**
     * Initializes the value for the {@link WindowScale#timestampCol() timestampCol} attribute.
     * @param timestampCol The value for timestampCol (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder timestampCol(@Nullable String timestampCol) {
      this.timestampCol = timestampCol;
      return this;
    }

    /**
     * Initializes the value for the {@link WindowScale#timescaleUnits() timescaleUnits} attribute.
     * @param timescaleUnits The value for timescaleUnits 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder timescaleUnits(long timescaleUnits) {
      this.timescaleUnits = timescaleUnits;
      initBits &= ~INIT_BIT_TIMESCALE_UNITS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableWindowScale ImmutableWindowScale}.
     * @return An immutable instance of WindowScale
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableWindowScale build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableWindowScale.validate(new ImmutableWindowScale(timestampCol, timescaleUnits));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TIMESCALE_UNITS) != 0) attributes.add("timescaleUnits");
      return "Cannot build WindowScale, some of required attributes are not set " + attributes;
    }
  }
}
