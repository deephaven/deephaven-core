package io.deephaven.api.agg.util;

import io.deephaven.api.ColumnName;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link PercentileOutput}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePercentileOutput.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutablePercentileOutput.of()}.
 */
@Generated(from = "PercentileOutput", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutablePercentileOutput extends PercentileOutput {
  private final double percentile;
  private final ColumnName output;

  private ImmutablePercentileOutput(double percentile, ColumnName output) {
    this.percentile = percentile;
    this.output = Objects.requireNonNull(output, "output");
  }

  private ImmutablePercentileOutput(ImmutablePercentileOutput original, double percentile, ColumnName output) {
    this.percentile = percentile;
    this.output = output;
  }

  /**
   * Percentile. Must be in range [0.0, 1.0].
   * @return The percentile
   */
  @Override
  public double percentile() {
    return percentile;
  }

  /**
   * Output {@link ColumnName column name}.
   * @return The output {@link ColumnName column name}
   */
  @Override
  public ColumnName output() {
    return output;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PercentileOutput#percentile() percentile} attribute.
   * A value strict bits equality used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for percentile
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePercentileOutput withPercentile(double value) {
    if (Double.doubleToLongBits(this.percentile) == Double.doubleToLongBits(value)) return this;
    return validate(new ImmutablePercentileOutput(this, value, this.output));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PercentileOutput#output() output} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for output
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePercentileOutput withOutput(ColumnName value) {
    if (this.output == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "output");
    return validate(new ImmutablePercentileOutput(this, this.percentile, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePercentileOutput} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePercentileOutput
        && equalTo(0, (ImmutablePercentileOutput) another);
  }

  private boolean equalTo(int synthetic, ImmutablePercentileOutput another) {
    return Double.doubleToLongBits(percentile) == Double.doubleToLongBits(another.percentile)
        && output.equals(another.output);
  }

  /**
   * Computes a hash code from attributes: {@code percentile}, {@code output}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Double.hashCode(percentile);
    h += (h << 5) + output.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code PercentileOutput} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "PercentileOutput{"
        + "percentile=" + percentile
        + ", output=" + output
        + "}";
  }

  /**
   * Construct a new immutable {@code PercentileOutput} instance.
   * @param percentile The value for the {@code percentile} attribute
   * @param output The value for the {@code output} attribute
   * @return An immutable PercentileOutput instance
   */
  public static ImmutablePercentileOutput of(double percentile, ColumnName output) {
    return validate(new ImmutablePercentileOutput(percentile, output));
  }

  private static ImmutablePercentileOutput validate(ImmutablePercentileOutput instance) {
    instance.checkPercentile();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link PercentileOutput} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PercentileOutput instance
   */
  public static ImmutablePercentileOutput copyOf(PercentileOutput instance) {
    if (instance instanceof ImmutablePercentileOutput) {
      return (ImmutablePercentileOutput) instance;
    }
    return ImmutablePercentileOutput.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutablePercentileOutput ImmutablePercentileOutput}.
   * <pre>
   * ImmutablePercentileOutput.builder()
   *    .percentile(double) // required {@link PercentileOutput#percentile() percentile}
   *    .output(io.deephaven.api.ColumnName) // required {@link PercentileOutput#output() output}
   *    .build();
   * </pre>
   * @return A new ImmutablePercentileOutput builder
   */
  public static ImmutablePercentileOutput.Builder builder() {
    return new ImmutablePercentileOutput.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePercentileOutput ImmutablePercentileOutput}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "PercentileOutput", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_PERCENTILE = 0x1L;
    private static final long INIT_BIT_OUTPUT = 0x2L;
    private long initBits = 0x3L;

    private double percentile;
    private @Nullable ColumnName output;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code PercentileOutput} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(PercentileOutput instance) {
      Objects.requireNonNull(instance, "instance");
      percentile(instance.percentile());
      output(instance.output());
      return this;
    }

    /**
     * Initializes the value for the {@link PercentileOutput#percentile() percentile} attribute.
     * @param percentile The value for percentile 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder percentile(double percentile) {
      this.percentile = percentile;
      initBits &= ~INIT_BIT_PERCENTILE;
      return this;
    }

    /**
     * Initializes the value for the {@link PercentileOutput#output() output} attribute.
     * @param output The value for output 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder output(ColumnName output) {
      this.output = Objects.requireNonNull(output, "output");
      initBits &= ~INIT_BIT_OUTPUT;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePercentileOutput ImmutablePercentileOutput}.
     * @return An immutable instance of PercentileOutput
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePercentileOutput build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutablePercentileOutput.validate(new ImmutablePercentileOutput(null, percentile, output));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PERCENTILE) != 0) attributes.add("percentile");
      if ((initBits & INIT_BIT_OUTPUT) != 0) attributes.add("output");
      return "Cannot build PercentileOutput, some of required attributes are not set " + attributes;
    }
  }
}
