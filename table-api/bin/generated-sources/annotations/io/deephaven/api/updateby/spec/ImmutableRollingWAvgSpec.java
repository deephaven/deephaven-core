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
 * Immutable implementation of {@link RollingWAvgSpec}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableRollingWAvgSpec.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableRollingWAvgSpec.of()}.
 */
@Generated(from = "RollingWAvgSpec", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableRollingWAvgSpec extends RollingWAvgSpec {
  private final String weightCol;

  private ImmutableRollingWAvgSpec(String weightCol) {
    this.weightCol = Objects.requireNonNull(weightCol, "weightCol");
  }

  private ImmutableRollingWAvgSpec(ImmutableRollingWAvgSpec original, String weightCol) {
    this.weightCol = weightCol;
  }

  /**
   * @return The value of the {@code weightCol} attribute
   */
  @Override
  public String weightCol() {
    return weightCol;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link RollingWAvgSpec#weightCol() weightCol} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for weightCol
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableRollingWAvgSpec withWeightCol(String value) {
    String newValue = Objects.requireNonNull(value, "weightCol");
    if (this.weightCol.equals(newValue)) return this;
    return new ImmutableRollingWAvgSpec(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableRollingWAvgSpec} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableRollingWAvgSpec
        && equalTo(0, (ImmutableRollingWAvgSpec) another);
  }

  private boolean equalTo(int synthetic, ImmutableRollingWAvgSpec another) {
    return weightCol.equals(another.weightCol);
  }

  /**
   * Computes a hash code from attributes: {@code weightCol}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + weightCol.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code RollingWAvgSpec} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "RollingWAvgSpec{"
        + "weightCol=" + weightCol
        + "}";
  }

  /**
   * Construct a new immutable {@code RollingWAvgSpec} instance.
   * @param weightCol The value for the {@code weightCol} attribute
   * @return An immutable RollingWAvgSpec instance
   */
  public static ImmutableRollingWAvgSpec of(String weightCol) {
    return new ImmutableRollingWAvgSpec(weightCol);
  }

  /**
   * Creates an immutable copy of a {@link RollingWAvgSpec} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RollingWAvgSpec instance
   */
  public static ImmutableRollingWAvgSpec copyOf(RollingWAvgSpec instance) {
    if (instance instanceof ImmutableRollingWAvgSpec) {
      return (ImmutableRollingWAvgSpec) instance;
    }
    return ImmutableRollingWAvgSpec.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
   * <pre>
   * ImmutableRollingWAvgSpec.builder()
   *    .weightCol(String) // required {@link RollingWAvgSpec#weightCol() weightCol}
   *    .build();
   * </pre>
   * @return A new ImmutableRollingWAvgSpec builder
   */
  public static ImmutableRollingWAvgSpec.Builder builder() {
    return new ImmutableRollingWAvgSpec.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "RollingWAvgSpec", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_WEIGHT_COL = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String weightCol;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code RollingWAvgSpec} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(RollingWAvgSpec instance) {
      Objects.requireNonNull(instance, "instance");
      weightCol(instance.weightCol());
      return this;
    }

    /**
     * Initializes the value for the {@link RollingWAvgSpec#weightCol() weightCol} attribute.
     * @param weightCol The value for weightCol 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder weightCol(String weightCol) {
      this.weightCol = Objects.requireNonNull(weightCol, "weightCol");
      initBits &= ~INIT_BIT_WEIGHT_COL;
      return this;
    }

    /**
     * Builds a new {@link ImmutableRollingWAvgSpec ImmutableRollingWAvgSpec}.
     * @return An immutable instance of RollingWAvgSpec
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableRollingWAvgSpec build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableRollingWAvgSpec(null, weightCol);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_WEIGHT_COL) != 0) attributes.add("weightCol");
      return "Cannot build RollingWAvgSpec, some of required attributes are not set " + attributes;
    }
  }
}
