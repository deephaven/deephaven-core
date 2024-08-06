package io.deephaven.json;

import java.time.Instant;
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
 * Immutable implementation of {@link InstantNumberValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableInstantNumberValue.builder()}.
 */
@Generated(from = "InstantNumberValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableInstantNumberValue extends InstantNumberValue {
  private final boolean allowMissing;
  private final @Nullable Instant onNull;
  private final @Nullable Instant onMissing;
  private final InstantNumberValue.Format format;
  private final Set<JsonValueTypes> allowedTypes;

  private ImmutableInstantNumberValue(ImmutableInstantNumberValue.Builder builder) {
    this.onNull = builder.onNull;
    this.onMissing = builder.onMissing;
    this.format = builder.format;
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

  private ImmutableInstantNumberValue(
      boolean allowMissing,
      @Nullable Instant onNull,
      @Nullable Instant onMissing,
      InstantNumberValue.Format format,
      Set<JsonValueTypes> allowedTypes) {
    this.allowMissing = allowMissing;
    this.onNull = onNull;
    this.onMissing = onMissing;
    this.format = format;
    this.allowedTypes = allowedTypes;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "InstantNumberValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableInstantNumberValue.super.allowMissing();
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
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableInstantNumberValue.super.allowedTypes());
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
      return "Cannot build InstantNumberValue, attribute initializers form cycle " + attributes;
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
  public Optional<Instant> onNull() {
    return Optional.ofNullable(onNull);
  }

  /**
   * @return The value of the {@code onMissing} attribute
   */
  @Override
  public Optional<Instant> onMissing() {
    return Optional.ofNullable(onMissing);
  }

  /**
   * The format to use.
   */
  @Override
  public InstantNumberValue.Format format() {
    return format;
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
   * Copy the current immutable object by setting a value for the {@link InstantNumberValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInstantNumberValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableInstantNumberValue(value, this.onNull, this.onMissing, this.format, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link InstantNumberValue#onNull() onNull} attribute.
   * @param value The value for onNull
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantNumberValue withOnNull(Instant value) {
    @Nullable Instant newValue = Objects.requireNonNull(value, "onNull");
    if (this.onNull == newValue) return this;
    return validate(new ImmutableInstantNumberValue(this.allowMissing, newValue, this.onMissing, this.format, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link InstantNumberValue#onNull() onNull} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNull
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableInstantNumberValue withOnNull(Optional<? extends Instant> optional) {
    @Nullable Instant value = optional.orElse(null);
    if (this.onNull == value) return this;
    return validate(new ImmutableInstantNumberValue(this.allowMissing, value, this.onMissing, this.format, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link InstantNumberValue#onMissing() onMissing} attribute.
   * @param value The value for onMissing
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantNumberValue withOnMissing(Instant value) {
    @Nullable Instant newValue = Objects.requireNonNull(value, "onMissing");
    if (this.onMissing == newValue) return this;
    return validate(new ImmutableInstantNumberValue(this.allowMissing, this.onNull, newValue, this.format, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link InstantNumberValue#onMissing() onMissing} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onMissing
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableInstantNumberValue withOnMissing(Optional<? extends Instant> optional) {
    @Nullable Instant value = optional.orElse(null);
    if (this.onMissing == value) return this;
    return validate(new ImmutableInstantNumberValue(this.allowMissing, this.onNull, value, this.format, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InstantNumberValue#format() format} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for format
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInstantNumberValue withFormat(InstantNumberValue.Format value) {
    InstantNumberValue.Format newValue = Objects.requireNonNull(value, "format");
    if (this.format == newValue) return this;
    return validate(new ImmutableInstantNumberValue(this.allowMissing, this.onNull, this.onMissing, newValue, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InstantNumberValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantNumberValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableInstantNumberValue(this.allowMissing, this.onNull, this.onMissing, this.format, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InstantNumberValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantNumberValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableInstantNumberValue(this.allowMissing, this.onNull, this.onMissing, this.format, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableInstantNumberValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableInstantNumberValue
        && equalTo(0, (ImmutableInstantNumberValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableInstantNumberValue another) {
    return allowMissing == another.allowMissing
        && Objects.equals(onNull, another.onNull)
        && Objects.equals(onMissing, another.onMissing)
        && format.equals(another.format)
        && allowedTypes.equals(another.allowedTypes);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code onNull}, {@code onMissing}, {@code format}, {@code allowedTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + Objects.hashCode(onNull);
    h += (h << 5) + Objects.hashCode(onMissing);
    h += (h << 5) + format.hashCode();
    h += (h << 5) + allowedTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code InstantNumberValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("InstantNumberValue{");
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
    builder.append("format=").append(format);
    builder.append(", ");
    builder.append("allowedTypes=").append(allowedTypes);
    return builder.append("}").toString();
  }

  private static ImmutableInstantNumberValue validate(ImmutableInstantNumberValue instance) {
    instance.checkOnMissing();
    instance.checkOnNull();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link InstantNumberValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable InstantNumberValue instance
   */
  public static ImmutableInstantNumberValue copyOf(InstantNumberValue instance) {
    if (instance instanceof ImmutableInstantNumberValue) {
      return (ImmutableInstantNumberValue) instance;
    }
    return ImmutableInstantNumberValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableInstantNumberValue ImmutableInstantNumberValue}.
   * <pre>
   * ImmutableInstantNumberValue.builder()
   *    .allowMissing(boolean) // optional {@link InstantNumberValue#allowMissing() allowMissing}
   *    .onNull(java.time.Instant) // optional {@link InstantNumberValue#onNull() onNull}
   *    .onMissing(java.time.Instant) // optional {@link InstantNumberValue#onMissing() onMissing}
   *    .format(io.deephaven.json.InstantNumberValue.Format) // required {@link InstantNumberValue#format() format}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link InstantNumberValue#allowedTypes() allowedTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableInstantNumberValue builder
   */
  public static ImmutableInstantNumberValue.Builder builder() {
    return new ImmutableInstantNumberValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableInstantNumberValue ImmutableInstantNumberValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "InstantNumberValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements InstantNumberValue.Builder {
    private static final long INIT_BIT_FORMAT = 0x1L;
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x2L;
    private long initBits = 0x1L;
    private long optBits;

    private boolean allowMissing;
    private @Nullable Instant onNull;
    private @Nullable Instant onMissing;
    private @Nullable InstantNumberValue.Format format;
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.Value} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(Value instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.InstantNumberValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(InstantNumberValue instance) {
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
      if (object instanceof InstantNumberValue) {
        InstantNumberValue instance = (InstantNumberValue) object;
        if ((bits & 0x1L) == 0) {
          addAllAllowedTypes(instance.allowedTypes());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          allowMissing(instance.allowMissing());
          bits |= 0x2L;
        }
        format(instance.format());
        Optional<Instant> onMissingOptional = instance.onMissing();
        if (onMissingOptional.isPresent()) {
          onMissing(onMissingOptional);
        }
        Optional<Instant> onNullOptional = instance.onNull();
        if (onNullOptional.isPresent()) {
          onNull(onNullOptional);
        }
      }
    }

    /**
     * Initializes the value for the {@link InstantNumberValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link InstantNumberValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Initializes the optional value {@link InstantNumberValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNull(Instant onNull) {
      this.onNull = Objects.requireNonNull(onNull, "onNull");
      return this;
    }

    /**
     * Initializes the optional value {@link InstantNumberValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNull(Optional<? extends Instant> onNull) {
      this.onNull = onNull.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link InstantNumberValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for chained invocation
     */
    public final Builder onMissing(Instant onMissing) {
      this.onMissing = Objects.requireNonNull(onMissing, "onMissing");
      return this;
    }

    /**
     * Initializes the optional value {@link InstantNumberValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onMissing(Optional<? extends Instant> onMissing) {
      this.onMissing = onMissing.orElse(null);
      return this;
    }

    /**
     * Initializes the value for the {@link InstantNumberValue#format() format} attribute.
     * @param format The value for format 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder format(InstantNumberValue.Format format) {
      this.format = Objects.requireNonNull(format, "format");
      initBits &= ~INIT_BIT_FORMAT;
      return this;
    }

    /**
     * Adds one element to {@link InstantNumberValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link InstantNumberValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link InstantNumberValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link InstantNumberValue#allowedTypes() allowedTypes} set.
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
     * Builds a new {@link ImmutableInstantNumberValue ImmutableInstantNumberValue}.
     * @return An immutable instance of InstantNumberValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableInstantNumberValue build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableInstantNumberValue.validate(new ImmutableInstantNumberValue(this));
    }

    private boolean allowMissingIsSet() {
      return (optBits & OPT_BIT_ALLOW_MISSING) != 0;
    }

    private boolean allowedTypesIsSet() {
      return (optBits & OPT_BIT_ALLOWED_TYPES) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FORMAT) != 0) attributes.add("format");
      return "Cannot build InstantNumberValue, some of required attributes are not set " + attributes;
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
