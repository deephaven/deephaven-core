package io.deephaven.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ObjectEntriesValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableObjectEntriesValue.builder()}.
 */
@Generated(from = "ObjectEntriesValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableObjectEntriesValue extends ObjectEntriesValue {
  private final boolean allowMissing;
  private final Value key;
  private final Value value;
  private final Set<JsonValueTypes> allowedTypes;

  private ImmutableObjectEntriesValue(ImmutableObjectEntriesValue.Builder builder) {
    this.value = builder.value;
    if (builder.allowMissingIsSet()) {
      initShim.allowMissing(builder.allowMissing);
    }
    if (builder.key != null) {
      initShim.key(builder.key);
    }
    if (builder.allowedTypesIsSet()) {
      initShim.allowedTypes(createUnmodifiableEnumSet(builder.allowedTypes));
    }
    this.allowMissing = initShim.allowMissing();
    this.key = initShim.key();
    this.allowedTypes = initShim.allowedTypes();
    this.initShim = null;
  }

  private ImmutableObjectEntriesValue(
      boolean allowMissing,
      Value key,
      Value value,
      Set<JsonValueTypes> allowedTypes) {
    this.allowMissing = allowMissing;
    this.key = key;
    this.value = value;
    this.allowedTypes = allowedTypes;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ObjectEntriesValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableObjectEntriesValue.super.allowMissing();
        allowMissingBuildStage = STAGE_INITIALIZED;
      }
      return this.allowMissing;
    }

    void allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      allowMissingBuildStage = STAGE_INITIALIZED;
    }

    private byte keyBuildStage = STAGE_UNINITIALIZED;
    private Value key;

    Value key() {
      if (keyBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (keyBuildStage == STAGE_UNINITIALIZED) {
        keyBuildStage = STAGE_INITIALIZING;
        this.key = Objects.requireNonNull(ImmutableObjectEntriesValue.super.key(), "key");
        keyBuildStage = STAGE_INITIALIZED;
      }
      return this.key;
    }

    void key(Value key) {
      this.key = key;
      keyBuildStage = STAGE_INITIALIZED;
    }

    private byte allowedTypesBuildStage = STAGE_UNINITIALIZED;
    private Set<JsonValueTypes> allowedTypes;

    Set<JsonValueTypes> allowedTypes() {
      if (allowedTypesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowedTypesBuildStage == STAGE_UNINITIALIZED) {
        allowedTypesBuildStage = STAGE_INITIALIZING;
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableObjectEntriesValue.super.allowedTypes());
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
      if (keyBuildStage == STAGE_INITIALIZING) attributes.add("key");
      if (allowedTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowedTypes");
      return "Cannot build ObjectEntriesValue, attribute initializers form cycle " + attributes;
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
   * The key options which must minimally support {@link JsonValueTypes#STRING}. By default is
   * {@link StringValue#standard()}.
   */
  @Override
  public Value key() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.key()
        : this.key;
  }

  /**
   * The value options.
   */
  @Override
  public Value value() {
    return value;
  }

  /**
   * {@inheritDoc} Must be a subset of {@link JsonValueTypes#objectOrNull()}. By default is
   * {@link JsonValueTypes#objectOrNull()}.
   */
  @Override
  public Set<JsonValueTypes> allowedTypes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowedTypes()
        : this.allowedTypes;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectEntriesValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectEntriesValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableObjectEntriesValue(value, this.key, this.value, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectEntriesValue#key() key} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for key
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectEntriesValue withKey(Value value) {
    if (this.key == value) return this;
    Value newValue = Objects.requireNonNull(value, "key");
    return validate(new ImmutableObjectEntriesValue(this.allowMissing, newValue, this.value, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectEntriesValue#value() value} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectEntriesValue withValue(Value value) {
    if (this.value == value) return this;
    Value newValue = Objects.requireNonNull(value, "value");
    return validate(new ImmutableObjectEntriesValue(this.allowMissing, this.key, newValue, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectEntriesValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectEntriesValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableObjectEntriesValue(this.allowMissing, this.key, this.value, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectEntriesValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectEntriesValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableObjectEntriesValue(this.allowMissing, this.key, this.value, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableObjectEntriesValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableObjectEntriesValue
        && equalTo(0, (ImmutableObjectEntriesValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableObjectEntriesValue another) {
    return allowMissing == another.allowMissing
        && key.equals(another.key)
        && value.equals(another.value)
        && allowedTypes.equals(another.allowedTypes);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code key}, {@code value}, {@code allowedTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + key.hashCode();
    h += (h << 5) + value.hashCode();
    h += (h << 5) + allowedTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ObjectEntriesValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ObjectEntriesValue{"
        + "allowMissing=" + allowMissing
        + ", key=" + key
        + ", value=" + value
        + ", allowedTypes=" + allowedTypes
        + "}";
  }

  private static ImmutableObjectEntriesValue validate(ImmutableObjectEntriesValue instance) {
    instance.checkKey();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ObjectEntriesValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ObjectEntriesValue instance
   */
  public static ImmutableObjectEntriesValue copyOf(ObjectEntriesValue instance) {
    if (instance instanceof ImmutableObjectEntriesValue) {
      return (ImmutableObjectEntriesValue) instance;
    }
    return ImmutableObjectEntriesValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableObjectEntriesValue ImmutableObjectEntriesValue}.
   * <pre>
   * ImmutableObjectEntriesValue.builder()
   *    .allowMissing(boolean) // optional {@link ObjectEntriesValue#allowMissing() allowMissing}
   *    .key(io.deephaven.json.Value) // optional {@link ObjectEntriesValue#key() key}
   *    .value(io.deephaven.json.Value) // required {@link ObjectEntriesValue#value() value}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link ObjectEntriesValue#allowedTypes() allowedTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableObjectEntriesValue builder
   */
  public static ImmutableObjectEntriesValue.Builder builder() {
    return new ImmutableObjectEntriesValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableObjectEntriesValue ImmutableObjectEntriesValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ObjectEntriesValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ObjectEntriesValue.Builder {
    private static final long INIT_BIT_VALUE = 0x1L;
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x2L;
    private long initBits = 0x1L;
    private long optBits;

    private boolean allowMissing;
    private @Nullable Value key;
    private @Nullable Value value;
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.ObjectEntriesValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ObjectEntriesValue instance) {
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
      if (object instanceof ObjectEntriesValue) {
        ObjectEntriesValue instance = (ObjectEntriesValue) object;
        value(instance.value());
        key(instance.key());
      }
    }

    /**
     * Initializes the value for the {@link ObjectEntriesValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectEntriesValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectEntriesValue#key() key} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectEntriesValue#key() key}.</em>
     * @param key The value for key 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder key(Value key) {
      this.key = Objects.requireNonNull(key, "key");
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectEntriesValue#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder value(Value value) {
      this.value = Objects.requireNonNull(value, "value");
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Adds one element to {@link ObjectEntriesValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link ObjectEntriesValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link ObjectEntriesValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link ObjectEntriesValue#allowedTypes() allowedTypes} set.
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
     * Builds a new {@link ImmutableObjectEntriesValue ImmutableObjectEntriesValue}.
     * @return An immutable instance of ObjectEntriesValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableObjectEntriesValue build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableObjectEntriesValue.validate(new ImmutableObjectEntriesValue(this));
    }

    private boolean allowMissingIsSet() {
      return (optBits & OPT_BIT_ALLOW_MISSING) != 0;
    }

    private boolean allowedTypesIsSet() {
      return (optBits & OPT_BIT_ALLOWED_TYPES) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build ObjectEntriesValue, some of required attributes are not set " + attributes;
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
