package io.deephaven.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LongValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLongValue.builder()}.
 */
@Generated(from = "LongValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableLongValue extends LongValue {
  private final boolean allowMissing;
  private final @Nullable Long onNull;
  private final @Nullable Long onMissing;
  private final Set<JsonValueTypes> allowedTypes;

  private ImmutableLongValue(ImmutableLongValue.Builder builder) {
    this.onNull = builder.onNull;
    this.onMissing = builder.onMissing;
    if (builder.allowMissingIsSet()) {
      initShim.allowMissing(builder.allowMissing);
    }
    if (builder.allowedTypesIsSet()) {
      initShim.allowedTypes(createUnmodifiableEnumSet(builder.allowedTypes));
    }
    this.allowMissing = initShim.allowMissing();
    this.allowedTypes = initShim.allowedTypes();
    this.initShim = null;
  }

  private ImmutableLongValue(
      boolean allowMissing,
      @Nullable Long onNull,
      @Nullable Long onMissing,
      Set<JsonValueTypes> allowedTypes) {
    this.allowMissing = allowMissing;
    this.onNull = onNull;
    this.onMissing = onMissing;
    this.allowedTypes = allowedTypes;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "LongValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableLongValue.super.allowMissing();
        allowMissingBuildStage = STAGE_INITIALIZED;
      }
      return this.allowMissing;
    }

    void allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      allowMissingBuildStage = STAGE_INITIALIZED;
    }

    private byte allowedTypesBuildStage = STAGE_UNINITIALIZED;
    private Set<JsonValueTypes> allowedTypes;

    Set<JsonValueTypes> allowedTypes() {
      if (allowedTypesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowedTypesBuildStage == STAGE_UNINITIALIZED) {
        allowedTypesBuildStage = STAGE_INITIALIZING;
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableLongValue.super.allowedTypes());
        allowedTypesBuildStage = STAGE_INITIALIZED;
      }
      return this.allowedTypes;
    }

    void allowedTypes(Set<JsonValueTypes> allowedTypes) {
      this.allowedTypes = allowedTypes;
      allowedTypesBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (allowMissingBuildStage == STAGE_INITIALIZING) attributes.add("allowMissing");
      if (allowedTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowedTypes");
      return "Cannot build LongValue, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * If the processor should allow a missing JSON value. By default is {@code true}.
   */
  @Override
  public boolean allowMissing() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowMissing()
        : this.allowMissing;
  }

  /**
   * @return The value of the {@code onNull} attribute
   */
  @Override
  public Optional<Long> onNull() {
    return Optional.ofNullable(onNull);
  }

  /**
   * @return The value of the {@code onMissing} attribute
   */
  @Override
  public Optional<Long> onMissing() {
    return Optional.ofNullable(onMissing);
  }

  /**
   * {@inheritDoc} Must be a subset of {@link JsonValueTypes#numberLike()}. By default is
   * {@link JsonValueTypes#intOrNull()}.
   */
  @Override
  public Set<JsonValueTypes> allowedTypes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowedTypes()
        : this.allowedTypes;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LongValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLongValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableLongValue(value, this.onNull, this.onMissing, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link LongValue#onNull() onNull} attribute.
   * @param value The value for onNull
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withOnNull(long value) {
    @Nullable Long newValue = value;
    if (Objects.equals(this.onNull, newValue)) return this;
    return validate(new ImmutableLongValue(this.allowMissing, newValue, this.onMissing, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link LongValue#onNull() onNull} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNull
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withOnNull(Optional<Long> optional) {
    @Nullable Long value = optional.orElse(null);
    if (Objects.equals(this.onNull, value)) return this;
    return validate(new ImmutableLongValue(this.allowMissing, value, this.onMissing, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link LongValue#onMissing() onMissing} attribute.
   * @param value The value for onMissing
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withOnMissing(long value) {
    @Nullable Long newValue = value;
    if (Objects.equals(this.onMissing, newValue)) return this;
    return validate(new ImmutableLongValue(this.allowMissing, this.onNull, newValue, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link LongValue#onMissing() onMissing} attribute.
   * An equality check is used on inner nullable value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onMissing
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withOnMissing(Optional<Long> optional) {
    @Nullable Long value = optional.orElse(null);
    if (Objects.equals(this.onMissing, value)) return this;
    return validate(new ImmutableLongValue(this.allowMissing, this.onNull, value, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link LongValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableLongValue(this.allowMissing, this.onNull, this.onMissing, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link LongValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableLongValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableLongValue(this.allowMissing, this.onNull, this.onMissing, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLongValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLongValue
        && equalTo(0, (ImmutableLongValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableLongValue another) {
    return allowMissing == another.allowMissing
        && Objects.equals(onNull, another.onNull)
        && Objects.equals(onMissing, another.onMissing)
        && allowedTypes.equals(another.allowedTypes);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code onNull}, {@code onMissing}, {@code allowedTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + Objects.hashCode(onNull);
    h += (h << 5) + Objects.hashCode(onMissing);
    h += (h << 5) + allowedTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code LongValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("LongValue{");
    builder.append("allowMissing=").append(allowMissing);
    if (onNull != null) {
      builder.append(", ");
      builder.append("onNull=").append(onNull);
    }
    if (onMissing != null) {
      builder.append(", ");
      builder.append("onMissing=").append(onMissing);
    }
    builder.append(", ");
    builder.append("allowedTypes=").append(allowedTypes);
    return builder.append("}").toString();
  }

  private static ImmutableLongValue validate(ImmutableLongValue instance) {
    instance.checkOnMissing();
    instance.checkOnNull();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LongValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LongValue instance
   */
  public static ImmutableLongValue copyOf(LongValue instance) {
    if (instance instanceof ImmutableLongValue) {
      return (ImmutableLongValue) instance;
    }
    return ImmutableLongValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLongValue ImmutableLongValue}.
   * <pre>
   * ImmutableLongValue.builder()
   *    .allowMissing(boolean) // optional {@link LongValue#allowMissing() allowMissing}
   *    .onNull(Long) // optional {@link LongValue#onNull() onNull}
   *    .onMissing(Long) // optional {@link LongValue#onMissing() onMissing}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link LongValue#allowedTypes() allowedTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableLongValue builder
   */
  public static ImmutableLongValue.Builder builder() {
    return new ImmutableLongValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLongValue ImmutableLongValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LongValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements LongValue.Builder {
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x2L;
    private long optBits;

    private boolean allowMissing;
    private @Nullable Long onNull;
    private @Nullable Long onMissing;
    private EnumSet<JsonValueTypes> allowedTypes = EnumSet.noneOf(JsonValueTypes.class);

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.ValueRestrictedUniverseBase} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ValueRestrictedUniverseBase instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.LongValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LongValue instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.Value} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Value instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof ValueRestrictedUniverseBase) {
        ValueRestrictedUniverseBase instance = (ValueRestrictedUniverseBase) object;
        if ((bits & 0x1L) == 0) {
          addAllAllowedTypes(instance.allowedTypes());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          allowMissing(instance.allowMissing());
          bits |= 0x2L;
        }
      }
      if (object instanceof LongValue) {
        LongValue instance = (LongValue) object;
        if ((bits & 0x1L) == 0) {
          addAllAllowedTypes(instance.allowedTypes());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          allowMissing(instance.allowMissing());
          bits |= 0x2L;
        }
        Optional<Long> onMissingOptional = instance.onMissing();
        if (onMissingOptional.isPresent()) {
          onMissing(onMissingOptional);
        }
        Optional<Long> onNullOptional = instance.onNull();
        if (onNullOptional.isPresent()) {
          onNull(onNullOptional);
        }
      }
      if (object instanceof Value) {
        Value instance = (Value) object;
        if ((bits & 0x1L) == 0) {
          addAllAllowedTypes(instance.allowedTypes());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          allowMissing(instance.allowMissing());
          bits |= 0x2L;
        }
      }
    }

    /**
     * Initializes the value for the {@link LongValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link LongValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Initializes the optional value {@link LongValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNull(long onNull) {
      this.onNull = onNull;
      return this;
    }

    /**
     * Initializes the optional value {@link LongValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNull(Optional<Long> onNull) {
      this.onNull = onNull.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link LongValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for chained invocation
     */
    public final Builder onMissing(long onMissing) {
      this.onMissing = onMissing;
      return this;
    }

    /**
     * Initializes the optional value {@link LongValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onMissing(Optional<Long> onMissing) {
      this.onMissing = onMissing.orElse(null);
      return this;
    }

    /**
     * Adds one element to {@link LongValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link LongValue#allowedTypes() allowedTypes} set.
     * @param elements An array of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes... elements) {
      for (JsonValueTypes element : elements) {
        this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      }
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }


    /**
     * Sets or replaces all elements for {@link LongValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link LongValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllAllowedTypes(Iterable<JsonValueTypes> elements) {
      for (JsonValueTypes element : elements) {
        this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      }
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLongValue ImmutableLongValue}.
     * @return An immutable instance of LongValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLongValue build() {
      return ImmutableLongValue.validate(new ImmutableLongValue(this));
    }

    private boolean allowMissingIsSet() {
      return (optBits & OPT_BIT_ALLOW_MISSING) != 0;
    }

    private boolean allowedTypesIsSet() {
      return (optBits & OPT_BIT_ALLOWED_TYPES) != 0;
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Enum<T>> Set<T> createUnmodifiableEnumSet(Iterable<T> iterable) {
    if (iterable instanceof EnumSet<?>) {
      return Collections.unmodifiableSet(EnumSet.copyOf((EnumSet<T>) iterable));
    }
    List<T> list = createSafeList(iterable, true, false);
    switch(list.size()) {
    case 0: return Collections.emptySet();
    case 1: return Collections.singleton(list.get(0));
    default: return Collections.unmodifiableSet(EnumSet.copyOf(list));
    }
  }
}
