package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link PairImpl}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePairImpl.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutablePairImpl.of()}.
 */
@Generated(from = "PairImpl", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutablePairImpl extends PairImpl {
  private final ColumnName input;
  private final ColumnName output;

  private ImmutablePairImpl(ColumnName input, ColumnName output) {
    this.input = Objects.requireNonNull(input, "input");
    this.output = Objects.requireNonNull(output, "output");
  }

  private ImmutablePairImpl(ImmutablePairImpl original, ColumnName input, ColumnName output) {
    this.input = input;
    this.output = output;
  }

  /**
   * @return The value of the {@code input} attribute
   */
  @Override
  public ColumnName input() {
    return input;
  }

  /**
   * @return The value of the {@code output} attribute
   */
  @Override
  public ColumnName output() {
    return output;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PairImpl#input() input} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for input
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePairImpl withInput(ColumnName value) {
    if (this.input == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "input");
    return validate(new ImmutablePairImpl(this, newValue, this.output));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link PairImpl#output() output} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for output
   * @return A modified copy of the {@code this} object
   */
  public final ImmutablePairImpl withOutput(ColumnName value) {
    if (this.output == value) return this;
    ColumnName newValue = Objects.requireNonNull(value, "output");
    return validate(new ImmutablePairImpl(this, this.input, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePairImpl} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePairImpl
        && equalTo(0, (ImmutablePairImpl) another);
  }

  private boolean equalTo(int synthetic, ImmutablePairImpl another) {
    return input.equals(another.input)
        && output.equals(another.output);
  }

  /**
   * Computes a hash code from attributes: {@code input}, {@code output}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + input.hashCode();
    h += (h << 5) + output.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code PairImpl} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "PairImpl{"
        + "input=" + input
        + ", output=" + output
        + "}";
  }

  /**
   * Construct a new immutable {@code PairImpl} instance.
   * @param input The value for the {@code input} attribute
   * @param output The value for the {@code output} attribute
   * @return An immutable PairImpl instance
   */
  public static ImmutablePairImpl of(ColumnName input, ColumnName output) {
    return validate(new ImmutablePairImpl(input, output));
  }

  private static ImmutablePairImpl validate(ImmutablePairImpl instance) {
    instance.checkNotSameColumns();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link PairImpl} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable PairImpl instance
   */
  public static ImmutablePairImpl copyOf(PairImpl instance) {
    if (instance instanceof ImmutablePairImpl) {
      return (ImmutablePairImpl) instance;
    }
    return ImmutablePairImpl.builder()
        .from(instance)
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutablePairImpl ImmutablePairImpl}.
   * <pre>
   * ImmutablePairImpl.builder()
   *    .input(io.deephaven.api.ColumnName) // required {@link PairImpl#input() input}
   *    .output(io.deephaven.api.ColumnName) // required {@link PairImpl#output() output}
   *    .build();
   * </pre>
   * @return A new ImmutablePairImpl builder
   */
  public static ImmutablePairImpl.Builder builder() {
    return new ImmutablePairImpl.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePairImpl ImmutablePairImpl}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "PairImpl", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_INPUT = 0x1L;
    private static final long INIT_BIT_OUTPUT = 0x2L;
    private long initBits = 0x3L;

    private @Nullable ColumnName input;
    private @Nullable ColumnName output;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.agg.Pair} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Pair instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.api.agg.PairImpl} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(PairImpl instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof Pair) {
        Pair instance = (Pair) object;
        if ((bits & 0x1L) == 0) {
          output(instance.output());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          input(instance.input());
          bits |= 0x2L;
        }
      }
      if (object instanceof PairImpl) {
        PairImpl instance = (PairImpl) object;
        if ((bits & 0x1L) == 0) {
          output(instance.output());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          input(instance.input());
          bits |= 0x2L;
        }
      }
    }

    /**
     * Initializes the value for the {@link PairImpl#input() input} attribute.
     * @param input The value for input 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder input(ColumnName input) {
      this.input = Objects.requireNonNull(input, "input");
      initBits &= ~INIT_BIT_INPUT;
      return this;
    }

    /**
     * Initializes the value for the {@link PairImpl#output() output} attribute.
     * @param output The value for output 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder output(ColumnName output) {
      this.output = Objects.requireNonNull(output, "output");
      initBits &= ~INIT_BIT_OUTPUT;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePairImpl ImmutablePairImpl}.
     * @return An immutable instance of PairImpl
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePairImpl build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutablePairImpl.validate(new ImmutablePairImpl(null, input, output));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_INPUT) != 0) attributes.add("input");
      if ((initBits & INIT_BIT_OUTPUT) != 0) attributes.add("output");
      return "Cannot build PairImpl, some of required attributes are not set " + attributes;
    }
  }
}
