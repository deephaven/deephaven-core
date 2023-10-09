package io.deephaven.process;

import java.util.Objects;
import java.util.OptionalLong;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link MemoryUsageInfo}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableMemoryUsageInfo.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableMemoryUsageInfo.of()}.
 */
@Generated(from = "MemoryUsageInfo", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableMemoryUsageInfo extends MemoryUsageInfo {
  private final Long init;
  private final Long max;

  private ImmutableMemoryUsageInfo(OptionalLong init, OptionalLong max) {
    this.init = init.isPresent() ? init.getAsLong() : null;
    this.max = max.isPresent() ? max.getAsLong() : null;
  }

  private ImmutableMemoryUsageInfo(ImmutableMemoryUsageInfo.Builder builder) {
    this.init = builder.init;
    this.max = builder.max;
  }

  /**
   * @return The value of the {@code init} attribute
   */
  @Override
  public OptionalLong init() {
    return init != null
        ? OptionalLong.of(init)
        : OptionalLong.empty();
  }

  /**
   * @return The value of the {@code max} attribute
   */
  @Override
  public OptionalLong max() {
    return max != null
        ? OptionalLong.of(max)
        : OptionalLong.empty();
  }

  /**
   * This instance is equal to all instances of {@code ImmutableMemoryUsageInfo} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableMemoryUsageInfo
        && equalTo((ImmutableMemoryUsageInfo) another);
  }

  private boolean equalTo(ImmutableMemoryUsageInfo another) {
    return Objects.equals(init, another.init)
        && Objects.equals(max, another.max);
  }

  /**
   * Computes a hash code from attributes: {@code init}, {@code max}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Objects.hashCode(init);
    h += (h << 5) + Objects.hashCode(max);
    return h;
  }


  /**
   * Prints the immutable value {@code MemoryUsageInfo} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("MemoryUsageInfo{");
    if (init != null) {
      builder.append("init=").append(init);
    }
    if (max != null) {
      if (builder.length() > 16) builder.append(", ");
      builder.append("max=").append(max);
    }
    return builder.append("}").toString();
  }

  /**
   * Construct a new immutable {@code MemoryUsageInfo} instance.
   * @param init The value for the {@code init} attribute
   * @param max The value for the {@code max} attribute
   * @return An immutable MemoryUsageInfo instance
   */
  public static ImmutableMemoryUsageInfo of(OptionalLong init, OptionalLong max) {
    return new ImmutableMemoryUsageInfo(init, max);
  }

  /**
   * Creates a builder for {@link ImmutableMemoryUsageInfo ImmutableMemoryUsageInfo}.
   * <pre>
   * ImmutableMemoryUsageInfo.builder()
   *    .init(long) // optional {@link MemoryUsageInfo#init() init}
   *    .max(long) // optional {@link MemoryUsageInfo#max() max}
   *    .build();
   * </pre>
   * @return A new ImmutableMemoryUsageInfo builder
   */
  public static ImmutableMemoryUsageInfo.Builder builder() {
    return new ImmutableMemoryUsageInfo.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableMemoryUsageInfo ImmutableMemoryUsageInfo}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "MemoryUsageInfo", generator = "Immutables")
  public static final class Builder {
    private static final long OPT_BIT_INIT = 0x1L;
    private static final long OPT_BIT_MAX = 0x2L;
    private long optBits;

    private Long init;
    private Long max;

    private Builder() {
    }

    /**
     * Initializes the optional value {@link MemoryUsageInfo#init() init} to init.
     * @param init The value for init
     * @return {@code this} builder for chained invocation
     */
    public final Builder init(long init) {
      checkNotIsSet(initIsSet(), "init");
      this.init = init;
      optBits |= OPT_BIT_INIT;
      return this;
    }

    /**
     * Initializes the optional value {@link MemoryUsageInfo#init() init} to init.
     * @param init The value for init
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder init(OptionalLong init) {
      checkNotIsSet(initIsSet(), "init");
      this.init = init.isPresent() ? init.getAsLong() : null;
      optBits |= OPT_BIT_INIT;
      return this;
    }

    /**
     * Initializes the optional value {@link MemoryUsageInfo#max() max} to max.
     * @param max The value for max
     * @return {@code this} builder for chained invocation
     */
    public final Builder max(long max) {
      checkNotIsSet(maxIsSet(), "max");
      this.max = max;
      optBits |= OPT_BIT_MAX;
      return this;
    }

    /**
     * Initializes the optional value {@link MemoryUsageInfo#max() max} to max.
     * @param max The value for max
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder max(OptionalLong max) {
      checkNotIsSet(maxIsSet(), "max");
      this.max = max.isPresent() ? max.getAsLong() : null;
      optBits |= OPT_BIT_MAX;
      return this;
    }

    /**
     * Builds a new {@link ImmutableMemoryUsageInfo ImmutableMemoryUsageInfo}.
     * @return An immutable instance of MemoryUsageInfo
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableMemoryUsageInfo build() {
      return new ImmutableMemoryUsageInfo(this);
    }

    private boolean initIsSet() {
      return (optBits & OPT_BIT_INIT) != 0;
    }

    private boolean maxIsSet() {
      return (optBits & OPT_BIT_MAX) != 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of MemoryUsageInfo is strict, attribute is already set: ".concat(name));
    }
  }
}
