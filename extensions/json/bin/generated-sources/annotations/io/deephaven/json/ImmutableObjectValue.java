package io.deephaven.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ObjectValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableObjectValue.builder()}.
 */
@Generated(from = "ObjectValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableObjectValue extends ObjectValue {
  private final boolean allowMissing;
  private final Set<ObjectField> fields;
  private final boolean allowUnknownFields;
  private final Set<JsonValueTypes> allowedTypes;

  private ImmutableObjectValue(ImmutableObjectValue.Builder builder) {
    this.fields = createUnmodifiableSet(builder.fields);
    if (builder.allowMissingIsSet()) {
      initShim.allowMissing(builder.allowMissing);
    }
    if (builder.allowUnknownFieldsIsSet()) {
      initShim.allowUnknownFields(builder.allowUnknownFields);
    }
    if (builder.allowedTypesIsSet()) {
      initShim.allowedTypes(createUnmodifiableEnumSet(builder.allowedTypes));
    }
    this.allowMissing = initShim.allowMissing();
    this.allowUnknownFields = initShim.allowUnknownFields();
    this.allowedTypes = initShim.allowedTypes();
    this.initShim = null;
  }

  private ImmutableObjectValue(
      boolean allowMissing,
      Set<ObjectField> fields,
      boolean allowUnknownFields,
      Set<JsonValueTypes> allowedTypes) {
    this.allowMissing = allowMissing;
    this.fields = fields;
    this.allowUnknownFields = allowUnknownFields;
    this.allowedTypes = allowedTypes;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ObjectValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableObjectValue.super.allowMissing();
        allowMissingBuildStage = STAGE_INITIALIZED;
      }
      return this.allowMissing;
    }

    void allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      allowMissingBuildStage = STAGE_INITIALIZED;
    }

    private byte allowUnknownFieldsBuildStage = STAGE_UNINITIALIZED;
    private boolean allowUnknownFields;

    boolean allowUnknownFields() {
      if (allowUnknownFieldsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowUnknownFieldsBuildStage == STAGE_UNINITIALIZED) {
        allowUnknownFieldsBuildStage = STAGE_INITIALIZING;
        this.allowUnknownFields = ImmutableObjectValue.super.allowUnknownFields();
        allowUnknownFieldsBuildStage = STAGE_INITIALIZED;
      }
      return this.allowUnknownFields;
    }

    void allowUnknownFields(boolean allowUnknownFields) {
      this.allowUnknownFields = allowUnknownFields;
      allowUnknownFieldsBuildStage = STAGE_INITIALIZED;
    }

    private byte allowedTypesBuildStage = STAGE_UNINITIALIZED;
    private Set<JsonValueTypes> allowedTypes;

    Set<JsonValueTypes> allowedTypes() {
      if (allowedTypesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowedTypesBuildStage == STAGE_UNINITIALIZED) {
        allowedTypesBuildStage = STAGE_INITIALIZING;
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableObjectValue.super.allowedTypes());
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
      if (allowUnknownFieldsBuildStage == STAGE_INITIALIZING) attributes.add("allowUnknownFields");
      if (allowedTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowedTypes");
      return "Cannot build ObjectValue, attribute initializers form cycle " + attributes;
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
   * The fields.
   */
  @Override
  public Set<ObjectField> fields() {
    return fields;
  }

  /**
   * If unknown fields are allowed. By default is {@code true}.
   */
  @Override
  public boolean allowUnknownFields() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowUnknownFields()
        : this.allowUnknownFields;
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
   * Copy the current immutable object by setting a value for the {@link ObjectValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableObjectValue(value, this.fields, this.allowUnknownFields, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectValue#fields() fields}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectValue withFields(ObjectField... elements) {
    Set<ObjectField> newValue = createUnmodifiableSet(createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableObjectValue(this.allowMissing, newValue, this.allowUnknownFields, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectValue#fields() fields}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of fields elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectValue withFields(Iterable<? extends ObjectField> elements) {
    if (this.fields == elements) return this;
    Set<ObjectField> newValue = createUnmodifiableSet(createSafeList(elements, true, false));
    return validate(new ImmutableObjectValue(this.allowMissing, newValue, this.allowUnknownFields, this.allowedTypes));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectValue#allowUnknownFields() allowUnknownFields} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowUnknownFields
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectValue withAllowUnknownFields(boolean value) {
    if (this.allowUnknownFields == value) return this;
    return validate(new ImmutableObjectValue(this.allowMissing, this.fields, value, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableObjectValue(this.allowMissing, this.fields, this.allowUnknownFields, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableObjectValue(this.allowMissing, this.fields, this.allowUnknownFields, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableObjectValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableObjectValue
        && equalTo(0, (ImmutableObjectValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableObjectValue another) {
    return allowMissing == another.allowMissing
        && fields.equals(another.fields)
        && allowUnknownFields == another.allowUnknownFields
        && allowedTypes.equals(another.allowedTypes);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code fields}, {@code allowUnknownFields}, {@code allowedTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + fields.hashCode();
    h += (h << 5) + Boolean.hashCode(allowUnknownFields);
    h += (h << 5) + allowedTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ObjectValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ObjectValue{"
        + "allowMissing=" + allowMissing
        + ", fields=" + fields
        + ", allowUnknownFields=" + allowUnknownFields
        + ", allowedTypes=" + allowedTypes
        + "}";
  }

  private static ImmutableObjectValue validate(ImmutableObjectValue instance) {
    instance.checkNonOverlapping();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ObjectValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ObjectValue instance
   */
  public static ImmutableObjectValue copyOf(ObjectValue instance) {
    if (instance instanceof ImmutableObjectValue) {
      return (ImmutableObjectValue) instance;
    }
    return ImmutableObjectValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableObjectValue ImmutableObjectValue}.
   * <pre>
   * ImmutableObjectValue.builder()
   *    .allowMissing(boolean) // optional {@link ObjectValue#allowMissing() allowMissing}
   *    .addFields|addAllFields(io.deephaven.json.ObjectField) // {@link ObjectValue#fields() fields} elements
   *    .allowUnknownFields(boolean) // optional {@link ObjectValue#allowUnknownFields() allowUnknownFields}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link ObjectValue#allowedTypes() allowedTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableObjectValue builder
   */
  public static ImmutableObjectValue.Builder builder() {
    return new ImmutableObjectValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableObjectValue ImmutableObjectValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ObjectValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ObjectValue.Builder {
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOW_UNKNOWN_FIELDS = 0x2L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x4L;
    private long optBits;

    private boolean allowMissing;
    private List<ObjectField> fields = new ArrayList<ObjectField>();
    private boolean allowUnknownFields;
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.ObjectValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ObjectValue instance) {
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
      if (object instanceof ObjectValue) {
        ObjectValue instance = (ObjectValue) object;
        addAllFields(instance.fields());
        allowUnknownFields(instance.allowUnknownFields());
      }
    }

    /**
     * Initializes the value for the {@link ObjectValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Adds one element to {@link ObjectValue#fields() fields} set.
     * @param element A fields element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFields(ObjectField element) {
      this.fields.add(Objects.requireNonNull(element, "fields element"));
      return this;
    }

    /**
     * Adds elements to {@link ObjectValue#fields() fields} set.
     * @param elements An array of fields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addFields(ObjectField... elements) {
      for (ObjectField element : elements) {
        this.fields.add(Objects.requireNonNull(element, "fields element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ObjectValue#fields() fields} set.
     * @param elements An iterable of fields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fields(Iterable<? extends ObjectField> elements) {
      this.fields.clear();
      return addAllFields(elements);
    }

    /**
     * Adds elements to {@link ObjectValue#fields() fields} set.
     * @param elements An iterable of fields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllFields(Iterable<? extends ObjectField> elements) {
      for (ObjectField element : elements) {
        this.fields.add(Objects.requireNonNull(element, "fields element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectValue#allowUnknownFields() allowUnknownFields} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectValue#allowUnknownFields() allowUnknownFields}.</em>
     * @param allowUnknownFields The value for allowUnknownFields 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowUnknownFields(boolean allowUnknownFields) {
      this.allowUnknownFields = allowUnknownFields;
      optBits |= OPT_BIT_ALLOW_UNKNOWN_FIELDS;
      return this;
    }

    /**
     * Adds one element to {@link ObjectValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link ObjectValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link ObjectValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link ObjectValue#allowedTypes() allowedTypes} set.
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
     * Builds a new {@link ImmutableObjectValue ImmutableObjectValue}.
     * @return An immutable instance of ObjectValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableObjectValue build() {
      return ImmutableObjectValue.validate(new ImmutableObjectValue(this));
    }

    private boolean allowMissingIsSet() {
      return (optBits & OPT_BIT_ALLOW_MISSING) != 0;
    }

    private boolean allowUnknownFieldsIsSet() {
      return (optBits & OPT_BIT_ALLOW_UNKNOWN_FIELDS) != 0;
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

  /** Unmodifiable set constructed from list to avoid rehashing. */
  private static <T> Set<T> createUnmodifiableSet(List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptySet();
    case 1: return Collections.singleton(list.get(0));
    default:
      Set<T> set = new LinkedHashSet<>(list.size());
      set.addAll(list);
      return Collections.unmodifiableSet(set);
    }
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
