package io.deephaven.json;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
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
 * Immutable implementation of {@link InstantValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableInstantValue.builder()}.
 */
@Generated(from = "InstantValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableInstantValue extends InstantValue {
  private final boolean allowMissing;
  private final @Nullable Instant onNull;
  private final @Nullable Instant onMissing;
  private final Set<JsonValueTypes> allowedTypes;
  private final DateTimeFormatter dateTimeFormatter;

  private ImmutableInstantValue(ImmutableInstantValue.Builder builder) {
    this.onNull = builder.onNull;
    this.onMissing = builder.onMissing;
    if (builder.allowMissingIsSet()) {
      initShim.allowMissing(builder.allowMissing);
    }
    if (builder.allowedTypesIsSet()) {
      initShim.allowedTypes(createUnmodifiableEnumSet(builder.allowedTypes));
    }
    if (builder.dateTimeFormatter != null) {
      initShim.dateTimeFormatter(builder.dateTimeFormatter);
    }
    this.allowMissing = initShim.allowMissing();
    this.allowedTypes = initShim.allowedTypes();
    this.dateTimeFormatter = initShim.dateTimeFormatter();
    this.initShim = null;
  }

  private ImmutableInstantValue(
      boolean allowMissing,
      @Nullable Instant onNull,
      @Nullable Instant onMissing,
      Set<JsonValueTypes> allowedTypes,
      DateTimeFormatter dateTimeFormatter) {
    this.allowMissing = allowMissing;
    this.onNull = onNull;
    this.onMissing = onMissing;
    this.allowedTypes = allowedTypes;
    this.dateTimeFormatter = dateTimeFormatter;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "InstantValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableInstantValue.super.allowMissing();
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
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableInstantValue.super.allowedTypes());
        allowedTypesBuildStage = STAGE_INITIALIZED;
      }
      return this.allowedTypes;
    }

    void allowedTypes(Set<JsonValueTypes> allowedTypes) {
      this.allowedTypes = allowedTypes;
      allowedTypesBuildStage = STAGE_INITIALIZED;
    }

    private byte dateTimeFormatterBuildStage = STAGE_UNINITIALIZED;
    private DateTimeFormatter dateTimeFormatter;

    DateTimeFormatter dateTimeFormatter() {
      if (dateTimeFormatterBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (dateTimeFormatterBuildStage == STAGE_UNINITIALIZED) {
        dateTimeFormatterBuildStage = STAGE_INITIALIZING;
        this.dateTimeFormatter = Objects.requireNonNull(ImmutableInstantValue.super.dateTimeFormatter(), "dateTimeFormatter");
        dateTimeFormatterBuildStage = STAGE_INITIALIZED;
      }
      return this.dateTimeFormatter;
    }

    void dateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
      this.dateTimeFormatter = dateTimeFormatter;
      dateTimeFormatterBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (allowMissingBuildStage == STAGE_INITIALIZING) attributes.add("allowMissing");
      if (allowedTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowedTypes");
      if (dateTimeFormatterBuildStage == STAGE_INITIALIZING) attributes.add("dateTimeFormatter");
      return "Cannot build InstantValue, attribute initializers form cycle " + attributes;
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
   * {@inheritDoc} Must be a subset of {@link JsonValueTypes#stringOrNull()}. By default is
   * {@link JsonValueTypes#stringOrNull()}.
   */
  @Override
  public Set<JsonValueTypes> allowedTypes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowedTypes()
        : this.allowedTypes;
  }

  /**
   * The date-time formatter to use for {@link DateTimeFormatter#parse(CharSequence) parsing}. The parsed result must
   * support extracting {@link java.time.temporal.ChronoField#INSTANT_SECONDS INSTANT_SECONDS} and
   * {@link java.time.temporal.ChronoField#NANO_OF_SECOND NANO_OF_SECOND} fields. Defaults to
   * {@link DateTimeFormatter#ISO_INSTANT} for java versions 12+, and {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}
   * otherwise. These defaults will parse offsets, converting to UTC as necessary.
   * @return the date-time formatter
   */
  @Override
  public DateTimeFormatter dateTimeFormatter() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.dateTimeFormatter()
        : this.dateTimeFormatter;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InstantValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInstantValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableInstantValue(value, this.onNull, this.onMissing, this.allowedTypes, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link InstantValue#onNull() onNull} attribute.
   * @param value The value for onNull
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantValue withOnNull(Instant value) {
    @Nullable Instant newValue = Objects.requireNonNull(value, "onNull");
    if (this.onNull == newValue) return this;
    return validate(new ImmutableInstantValue(this.allowMissing, newValue, this.onMissing, this.allowedTypes, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link InstantValue#onNull() onNull} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNull
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableInstantValue withOnNull(Optional<? extends Instant> optional) {
    @Nullable Instant value = optional.orElse(null);
    if (this.onNull == value) return this;
    return validate(new ImmutableInstantValue(this.allowMissing, value, this.onMissing, this.allowedTypes, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link InstantValue#onMissing() onMissing} attribute.
   * @param value The value for onMissing
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantValue withOnMissing(Instant value) {
    @Nullable Instant newValue = Objects.requireNonNull(value, "onMissing");
    if (this.onMissing == newValue) return this;
    return validate(new ImmutableInstantValue(this.allowMissing, this.onNull, newValue, this.allowedTypes, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link InstantValue#onMissing() onMissing} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onMissing
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableInstantValue withOnMissing(Optional<? extends Instant> optional) {
    @Nullable Instant value = optional.orElse(null);
    if (this.onMissing == value) return this;
    return validate(new ImmutableInstantValue(this.allowMissing, this.onNull, value, this.allowedTypes, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InstantValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableInstantValue(this.allowMissing, this.onNull, this.onMissing, newValue, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InstantValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInstantValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableInstantValue(this.allowMissing, this.onNull, this.onMissing, newValue, this.dateTimeFormatter));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InstantValue#dateTimeFormatter() dateTimeFormatter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for dateTimeFormatter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInstantValue withDateTimeFormatter(DateTimeFormatter value) {
    if (this.dateTimeFormatter == value) return this;
    DateTimeFormatter newValue = Objects.requireNonNull(value, "dateTimeFormatter");
    return validate(new ImmutableInstantValue(this.allowMissing, this.onNull, this.onMissing, this.allowedTypes, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableInstantValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableInstantValue
        && equalTo(0, (ImmutableInstantValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableInstantValue another) {
    return allowMissing == another.allowMissing
        && Objects.equals(onNull, another.onNull)
        && Objects.equals(onMissing, another.onMissing)
        && allowedTypes.equals(another.allowedTypes)
        && dateTimeFormatter.equals(another.dateTimeFormatter);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code onNull}, {@code onMissing}, {@code allowedTypes}, {@code dateTimeFormatter}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + Objects.hashCode(onNull);
    h += (h << 5) + Objects.hashCode(onMissing);
    h += (h << 5) + allowedTypes.hashCode();
    h += (h << 5) + dateTimeFormatter.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code InstantValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("InstantValue{");
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
    builder.append(", ");
    builder.append("dateTimeFormatter=").append(dateTimeFormatter);
    return builder.append("}").toString();
  }

  private static ImmutableInstantValue validate(ImmutableInstantValue instance) {
    instance.checkOnMissing();
    instance.checkOnNull();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link InstantValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable InstantValue instance
   */
  public static ImmutableInstantValue copyOf(InstantValue instance) {
    if (instance instanceof ImmutableInstantValue) {
      return (ImmutableInstantValue) instance;
    }
    return ImmutableInstantValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableInstantValue ImmutableInstantValue}.
   * <pre>
   * ImmutableInstantValue.builder()
   *    .allowMissing(boolean) // optional {@link InstantValue#allowMissing() allowMissing}
   *    .onNull(java.time.Instant) // optional {@link InstantValue#onNull() onNull}
   *    .onMissing(java.time.Instant) // optional {@link InstantValue#onMissing() onMissing}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link InstantValue#allowedTypes() allowedTypes} elements
   *    .dateTimeFormatter(java.time.format.DateTimeFormatter) // optional {@link InstantValue#dateTimeFormatter() dateTimeFormatter}
   *    .build();
   * </pre>
   * @return A new ImmutableInstantValue builder
   */
  public static ImmutableInstantValue.Builder builder() {
    return new ImmutableInstantValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableInstantValue ImmutableInstantValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "InstantValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements InstantValue.Builder {
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x2L;
    private long optBits;

    private boolean allowMissing;
    private @Nullable Instant onNull;
    private @Nullable Instant onMissing;
    private EnumSet<JsonValueTypes> allowedTypes = EnumSet.noneOf(JsonValueTypes.class);
    private @Nullable DateTimeFormatter dateTimeFormatter;

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
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.InstantValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(InstantValue instance) {
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
      if (object instanceof InstantValue) {
        InstantValue instance = (InstantValue) object;
        if ((bits & 0x1L) == 0) {
          addAllAllowedTypes(instance.allowedTypes());
          bits |= 0x1L;
        }
        if ((bits & 0x2L) == 0) {
          allowMissing(instance.allowMissing());
          bits |= 0x2L;
        }
        dateTimeFormatter(instance.dateTimeFormatter());
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
     * Initializes the value for the {@link InstantValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link InstantValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Initializes the optional value {@link InstantValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNull(Instant onNull) {
      this.onNull = Objects.requireNonNull(onNull, "onNull");
      return this;
    }

    /**
     * Initializes the optional value {@link InstantValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNull(Optional<? extends Instant> onNull) {
      this.onNull = onNull.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link InstantValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for chained invocation
     */
    public final Builder onMissing(Instant onMissing) {
      this.onMissing = Objects.requireNonNull(onMissing, "onMissing");
      return this;
    }

    /**
     * Initializes the optional value {@link InstantValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onMissing(Optional<? extends Instant> onMissing) {
      this.onMissing = onMissing.orElse(null);
      return this;
    }

    /**
     * Adds one element to {@link InstantValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link InstantValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link InstantValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link InstantValue#allowedTypes() allowedTypes} set.
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
     * Initializes the value for the {@link InstantValue#dateTimeFormatter() dateTimeFormatter} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link InstantValue#dateTimeFormatter() dateTimeFormatter}.</em>
     * @param dateTimeFormatter The value for dateTimeFormatter 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder dateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
      this.dateTimeFormatter = Objects.requireNonNull(dateTimeFormatter, "dateTimeFormatter");
      return this;
    }

    /**
     * Builds a new {@link ImmutableInstantValue ImmutableInstantValue}.
     * @return An immutable instance of InstantValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableInstantValue build() {
      return ImmutableInstantValue.validate(new ImmutableInstantValue(this));
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
