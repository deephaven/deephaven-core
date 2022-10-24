package io.deephaven.engine.table.impl.by.rollup;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link Partition}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutablePartition.builder()}.
 */
@Generated(from = "Partition", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
final class ImmutablePartition extends Partition {
  private final boolean includeConstituents;

  private ImmutablePartition(ImmutablePartition.Builder builder) {
    this.includeConstituents = builder.includeConstituents;
  }

  /**
   * @return The value of the {@code includeConstituents} attribute
   */
  @Override
  public boolean includeConstituents() {
    return includeConstituents;
  }

  /**
   * This instance is equal to all instances of {@code ImmutablePartition} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutablePartition
        && equalTo((ImmutablePartition) another);
  }

  private boolean equalTo(ImmutablePartition another) {
    return includeConstituents == another.includeConstituents;
  }

  /**
   * Computes a hash code from attributes: {@code includeConstituents}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(includeConstituents);
    return h;
  }


  /**
   * Prints the immutable value {@code Partition} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "Partition{"
        + "includeConstituents=" + includeConstituents
        + "}";
  }

  /**
   * Creates a builder for {@link ImmutablePartition ImmutablePartition}.
   * <pre>
   * ImmutablePartition.builder()
   *    .includeConstituents(boolean) // required {@link Partition#includeConstituents() includeConstituents}
   *    .build();
   * </pre>
   * @return A new ImmutablePartition builder
   */
  public static ImmutablePartition.Builder builder() {
    return new ImmutablePartition.Builder();
  }

  /**
   * Builds instances of type {@link ImmutablePartition ImmutablePartition}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "Partition", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_INCLUDE_CONSTITUENTS = 0x1L;
    private long initBits = 0x1L;

    private boolean includeConstituents;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link Partition#includeConstituents() includeConstituents} attribute.
     * @param includeConstituents The value for includeConstituents 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder includeConstituents(boolean includeConstituents) {
      checkNotIsSet(includeConstituentsIsSet(), "includeConstituents");
      this.includeConstituents = includeConstituents;
      initBits &= ~INIT_BIT_INCLUDE_CONSTITUENTS;
      return this;
    }

    /**
     * Builds a new {@link ImmutablePartition ImmutablePartition}.
     * @return An immutable instance of Partition
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutablePartition build() {
      checkRequiredAttributes();
      return new ImmutablePartition(this);
    }

    private boolean includeConstituentsIsSet() {
      return (initBits & INIT_BIT_INCLUDE_CONSTITUENTS) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of Partition is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!includeConstituentsIsSet()) attributes.add("includeConstituents");
      return "Cannot build Partition, some of required attributes are not set " + attributes;
    }
  }
}
