package io.deephaven.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TypedObjectValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTypedObjectValue.builder()}.
 */
@Generated(from = "TypedObjectValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableTypedObjectValue extends TypedObjectValue {
  private final boolean allowMissing;
  private final ObjectField typeField;
  private final Set<ObjectField> sharedFields;
  private final Map<Object, ObjectValue> objects;
  private final boolean allowUnknownTypes;
  private final Set<JsonValueTypes> allowedTypes;
  private final @Nullable Object onNull;
  private final @Nullable Object onMissing;

  private ImmutableTypedObjectValue(ImmutableTypedObjectValue.Builder builder) {
    this.typeField = builder.typeField;
    this.sharedFields = createUnmodifiableSet(builder.sharedFields);
    this.objects = createUnmodifiableMap(false, false, builder.objects);
    this.onNull = builder.onNull;
    this.onMissing = builder.onMissing;
    if (builder.allowMissingIsSet()) {
      initShim.allowMissing(builder.allowMissing);
    }
    if (builder.allowUnknownTypesIsSet()) {
      initShim.allowUnknownTypes(builder.allowUnknownTypes);
    }
    if (builder.allowedTypesIsSet()) {
      initShim.allowedTypes(createUnmodifiableEnumSet(builder.allowedTypes));
    }
    this.allowMissing = initShim.allowMissing();
    this.allowUnknownTypes = initShim.allowUnknownTypes();
    this.allowedTypes = initShim.allowedTypes();
    this.initShim = null;
  }

  private ImmutableTypedObjectValue(
      boolean allowMissing,
      ObjectField typeField,
      Set<ObjectField> sharedFields,
      Map<Object, ObjectValue> objects,
      boolean allowUnknownTypes,
      Set<JsonValueTypes> allowedTypes,
      @Nullable Object onNull,
      @Nullable Object onMissing) {
    this.allowMissing = allowMissing;
    this.typeField = typeField;
    this.sharedFields = sharedFields;
    this.objects = objects;
    this.allowUnknownTypes = allowUnknownTypes;
    this.allowedTypes = allowedTypes;
    this.onNull = onNull;
    this.onMissing = onMissing;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "TypedObjectValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableTypedObjectValue.super.allowMissing();
        allowMissingBuildStage = STAGE_INITIALIZED;
      }
      return this.allowMissing;
    }

    void allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      allowMissingBuildStage = STAGE_INITIALIZED;
    }

    private byte allowUnknownTypesBuildStage = STAGE_UNINITIALIZED;
    private boolean allowUnknownTypes;

    boolean allowUnknownTypes() {
      if (allowUnknownTypesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowUnknownTypesBuildStage == STAGE_UNINITIALIZED) {
        allowUnknownTypesBuildStage = STAGE_INITIALIZING;
        this.allowUnknownTypes = ImmutableTypedObjectValue.super.allowUnknownTypes();
        allowUnknownTypesBuildStage = STAGE_INITIALIZED;
      }
      return this.allowUnknownTypes;
    }

    void allowUnknownTypes(boolean allowUnknownTypes) {
      this.allowUnknownTypes = allowUnknownTypes;
      allowUnknownTypesBuildStage = STAGE_INITIALIZED;
    }

    private byte allowedTypesBuildStage = STAGE_UNINITIALIZED;
    private Set<JsonValueTypes> allowedTypes;

    Set<JsonValueTypes> allowedTypes() {
      if (allowedTypesBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowedTypesBuildStage == STAGE_UNINITIALIZED) {
        allowedTypesBuildStage = STAGE_INITIALIZING;
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableTypedObjectValue.super.allowedTypes());
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
      if (allowUnknownTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowUnknownTypes");
      if (allowedTypesBuildStage == STAGE_INITIALIZING) attributes.add("allowedTypes");
      return "Cannot build TypedObjectValue, attribute initializers form cycle " + attributes;
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
   * The type field.
   */
  @Override
  public ObjectField typeField() {
    return typeField;
  }

  /**
   * The shared fields.
   */
  @Override
  public Set<ObjectField> sharedFields() {
    return sharedFields;
  }

  /**
   * The discriminated objects.
   */
  @Override
  public Map<Object, ObjectValue> objects() {
    return objects;
  }

  /**
   * If unknown fields are allowed. By default is {@code true}.
   */
  @Override
  public boolean allowUnknownTypes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowUnknownTypes()
        : this.allowUnknownTypes;
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
   * The output type value to use when {@link JsonValueTypes#NULL} is encountered. {@link #allowedTypes()} must
   * contain {@link JsonValueTypes#NULL}.
   */
  @Override
  public Optional<Object> onNull() {
    return Optional.ofNullable(onNull);
  }

  /**
   * The output type value to use when a value is missing. {@link #allowMissing()} must be {@code true}.
   */
  @Override
  public Optional<Object> onMissing() {
    return Optional.ofNullable(onMissing);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TypedObjectValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTypedObjectValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableTypedObjectValue(
        value,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TypedObjectValue#typeField() typeField} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for typeField
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTypedObjectValue withTypeField(ObjectField value) {
    if (this.typeField == value) return this;
    ObjectField newValue = Objects.requireNonNull(value, "typeField");
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        newValue,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TypedObjectValue#sharedFields() sharedFields}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withSharedFields(ObjectField... elements) {
    Set<ObjectField> newValue = createUnmodifiableSet(createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        newValue,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TypedObjectValue#sharedFields() sharedFields}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of sharedFields elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withSharedFields(Iterable<? extends ObjectField> elements) {
    if (this.sharedFields == elements) return this;
    Set<ObjectField> newValue = createUnmodifiableSet(createSafeList(elements, true, false));
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        newValue,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by replacing the {@link TypedObjectValue#objects() objects} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the objects map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withObjects(Map<? extends Object, ? extends ObjectValue> entries) {
    if (this.objects == entries) return this;
    Map<Object, ObjectValue> newValue = createUnmodifiableMap(true, false, entries);
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        newValue,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TypedObjectValue#allowUnknownTypes() allowUnknownTypes} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowUnknownTypes
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTypedObjectValue withAllowUnknownTypes(boolean value) {
    if (this.allowUnknownTypes == value) return this;
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        value,
        this.allowedTypes,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TypedObjectValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        newValue,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TypedObjectValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        newValue,
        this.onNull,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link TypedObjectValue#onNull() onNull} attribute.
   * @param value The value for onNull
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withOnNull(Object value) {
    @Nullable Object newValue = Objects.requireNonNull(value, "onNull");
    if (this.onNull == newValue) return this;
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        newValue,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link TypedObjectValue#onNull() onNull} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onNull
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableTypedObjectValue withOnNull(Optional<? extends Object> optional) {
    @Nullable Object value = optional.orElse(null);
    if (this.onNull == value) return this;
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        value,
        this.onMissing));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link TypedObjectValue#onMissing() onMissing} attribute.
   * @param value The value for onMissing
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTypedObjectValue withOnMissing(Object value) {
    @Nullable Object newValue = Objects.requireNonNull(value, "onMissing");
    if (this.onMissing == newValue) return this;
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link TypedObjectValue#onMissing() onMissing} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for onMissing
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableTypedObjectValue withOnMissing(Optional<? extends Object> optional) {
    @Nullable Object value = optional.orElse(null);
    if (this.onMissing == value) return this;
    return validate(new ImmutableTypedObjectValue(
        this.allowMissing,
        this.typeField,
        this.sharedFields,
        this.objects,
        this.allowUnknownTypes,
        this.allowedTypes,
        this.onNull,
        value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTypedObjectValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTypedObjectValue
        && equalTo(0, (ImmutableTypedObjectValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableTypedObjectValue another) {
    return allowMissing == another.allowMissing
        && typeField.equals(another.typeField)
        && sharedFields.equals(another.sharedFields)
        && objects.equals(another.objects)
        && allowUnknownTypes == another.allowUnknownTypes
        && allowedTypes.equals(another.allowedTypes)
        && Objects.equals(onNull, another.onNull)
        && Objects.equals(onMissing, another.onMissing);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code typeField}, {@code sharedFields}, {@code objects}, {@code allowUnknownTypes}, {@code allowedTypes}, {@code onNull}, {@code onMissing}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + typeField.hashCode();
    h += (h << 5) + sharedFields.hashCode();
    h += (h << 5) + objects.hashCode();
    h += (h << 5) + Boolean.hashCode(allowUnknownTypes);
    h += (h << 5) + allowedTypes.hashCode();
    h += (h << 5) + Objects.hashCode(onNull);
    h += (h << 5) + Objects.hashCode(onMissing);
    return h;
  }

  /**
   * Prints the immutable value {@code TypedObjectValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("TypedObjectValue{");
    builder.append("allowMissing=").append(allowMissing);
    builder.append(", ");
    builder.append("typeField=").append(typeField);
    builder.append(", ");
    builder.append("sharedFields=").append(sharedFields);
    builder.append(", ");
    builder.append("objects=").append(objects);
    builder.append(", ");
    builder.append("allowUnknownTypes=").append(allowUnknownTypes);
    builder.append(", ");
    builder.append("allowedTypes=").append(allowedTypes);
    if (onNull != null) {
      builder.append(", ");
      builder.append("onNull=").append(onNull);
    }
    if (onMissing != null) {
      builder.append(", ");
      builder.append("onMissing=").append(onMissing);
    }
    return builder.append("}").toString();
  }

  private static ImmutableTypedObjectValue validate(ImmutableTypedObjectValue instance) {
    instance.checkOnMissing();
    instance.checkOnNull();
    instance.checkAllowedTypes();
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TypedObjectValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TypedObjectValue instance
   */
  public static ImmutableTypedObjectValue copyOf(TypedObjectValue instance) {
    if (instance instanceof ImmutableTypedObjectValue) {
      return (ImmutableTypedObjectValue) instance;
    }
    return ImmutableTypedObjectValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTypedObjectValue ImmutableTypedObjectValue}.
   * <pre>
   * ImmutableTypedObjectValue.builder()
   *    .allowMissing(boolean) // optional {@link TypedObjectValue#allowMissing() allowMissing}
   *    .typeField(io.deephaven.json.ObjectField) // required {@link TypedObjectValue#typeField() typeField}
   *    .addSharedFields|addAllSharedFields(io.deephaven.json.ObjectField) // {@link TypedObjectValue#sharedFields() sharedFields} elements
   *    .putObjects|putAllObjects(Object =&gt; io.deephaven.json.ObjectValue) // {@link TypedObjectValue#objects() objects} mappings
   *    .allowUnknownTypes(boolean) // optional {@link TypedObjectValue#allowUnknownTypes() allowUnknownTypes}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link TypedObjectValue#allowedTypes() allowedTypes} elements
   *    .onNull(Object) // optional {@link TypedObjectValue#onNull() onNull}
   *    .onMissing(Object) // optional {@link TypedObjectValue#onMissing() onMissing}
   *    .build();
   * </pre>
   * @return A new ImmutableTypedObjectValue builder
   */
  public static ImmutableTypedObjectValue.Builder builder() {
    return new ImmutableTypedObjectValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTypedObjectValue ImmutableTypedObjectValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TypedObjectValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements TypedObjectValue.Builder {
    private static final long INIT_BIT_TYPE_FIELD = 0x1L;
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOW_UNKNOWN_TYPES = 0x2L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x4L;
    private long initBits = 0x1L;
    private long optBits;

    private boolean allowMissing;
    private @Nullable ObjectField typeField;
    private List<ObjectField> sharedFields = new ArrayList<ObjectField>();
    private Map<Object, ObjectValue> objects = new LinkedHashMap<Object, ObjectValue>();
    private boolean allowUnknownTypes;
    private EnumSet<JsonValueTypes> allowedTypes = EnumSet.noneOf(JsonValueTypes.class);
    private @Nullable Object onNull;
    private @Nullable Object onMissing;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.TypedObjectValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TypedObjectValue instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
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

    private void from(Object object) {
      long bits = 0;
      if (object instanceof TypedObjectValue) {
        TypedObjectValue instance = (TypedObjectValue) object;
        allowUnknownTypes(instance.allowUnknownTypes());
        Optional<Object> onMissingOptional = instance.onMissing();
        if (onMissingOptional.isPresent()) {
          onMissing(onMissingOptional);
        }
        Optional<Object> onNullOptional = instance.onNull();
        if (onNullOptional.isPresent()) {
          onNull(onNullOptional);
        }
        addAllSharedFields(instance.sharedFields());
        putAllObjects(instance.objects());
        typeField(instance.typeField());
      }
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
    }

    /**
     * Initializes the value for the {@link TypedObjectValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TypedObjectValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Initializes the value for the {@link TypedObjectValue#typeField() typeField} attribute.
     * @param typeField The value for typeField 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder typeField(ObjectField typeField) {
      this.typeField = Objects.requireNonNull(typeField, "typeField");
      initBits &= ~INIT_BIT_TYPE_FIELD;
      return this;
    }

    /**
     * Adds one element to {@link TypedObjectValue#sharedFields() sharedFields} set.
     * @param element A sharedFields element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addSharedFields(ObjectField element) {
      this.sharedFields.add(Objects.requireNonNull(element, "sharedFields element"));
      return this;
    }

    /**
     * Adds elements to {@link TypedObjectValue#sharedFields() sharedFields} set.
     * @param elements An array of sharedFields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addSharedFields(ObjectField... elements) {
      for (ObjectField element : elements) {
        this.sharedFields.add(Objects.requireNonNull(element, "sharedFields element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link TypedObjectValue#sharedFields() sharedFields} set.
     * @param elements An iterable of sharedFields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder sharedFields(Iterable<? extends ObjectField> elements) {
      this.sharedFields.clear();
      return addAllSharedFields(elements);
    }

    /**
     * Adds elements to {@link TypedObjectValue#sharedFields() sharedFields} set.
     * @param elements An iterable of sharedFields elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllSharedFields(Iterable<? extends ObjectField> elements) {
      for (ObjectField element : elements) {
        this.sharedFields.add(Objects.requireNonNull(element, "sharedFields element"));
      }
      return this;
    }

    /**
     * Put one entry to the {@link TypedObjectValue#objects() objects} map.
     * @param key The key in the objects map
     * @param value The associated value in the objects map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putObjects(Object key, ObjectValue value) {
      this.objects.put(
          Objects.requireNonNull(key, "objects key"),
          value == null ? Objects.requireNonNull(value, "objects value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link TypedObjectValue#objects() objects} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putObjects(Map.Entry<? extends Object, ? extends ObjectValue> entry) {
      Object k = entry.getKey();
      ObjectValue v = entry.getValue();
      this.objects.put(
          Objects.requireNonNull(k, "objects key"),
          v == null ? Objects.requireNonNull(v, "objects value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link TypedObjectValue#objects() objects} map. Nulls are not permitted
     * @param entries The entries that will be added to the objects map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder objects(Map<? extends Object, ? extends ObjectValue> entries) {
      this.objects.clear();
      return putAllObjects(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link TypedObjectValue#objects() objects} map. Nulls are not permitted
     * @param entries The entries that will be added to the objects map
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder putAllObjects(Map<? extends Object, ? extends ObjectValue> entries) {
      for (Map.Entry<? extends Object, ? extends ObjectValue> e : entries.entrySet()) {
        Object k = e.getKey();
        ObjectValue v = e.getValue();
        this.objects.put(
            Objects.requireNonNull(k, "objects key"),
            v == null ? Objects.requireNonNull(v, "objects value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link TypedObjectValue#allowUnknownTypes() allowUnknownTypes} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TypedObjectValue#allowUnknownTypes() allowUnknownTypes}.</em>
     * @param allowUnknownTypes The value for allowUnknownTypes 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowUnknownTypes(boolean allowUnknownTypes) {
      this.allowUnknownTypes = allowUnknownTypes;
      optBits |= OPT_BIT_ALLOW_UNKNOWN_TYPES;
      return this;
    }

    /**
     * Adds one element to {@link TypedObjectValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link TypedObjectValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link TypedObjectValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link TypedObjectValue#allowedTypes() allowedTypes} set.
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
     * Initializes the optional value {@link TypedObjectValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for chained invocation
     */
    public final Builder onNull(Object onNull) {
      this.onNull = Objects.requireNonNull(onNull, "onNull");
      return this;
    }

    /**
     * Initializes the optional value {@link TypedObjectValue#onNull() onNull} to onNull.
     * @param onNull The value for onNull
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onNull(Optional<? extends Object> onNull) {
      this.onNull = onNull.orElse(null);
      return this;
    }

    /**
     * Initializes the optional value {@link TypedObjectValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for chained invocation
     */
    public final Builder onMissing(Object onMissing) {
      this.onMissing = Objects.requireNonNull(onMissing, "onMissing");
      return this;
    }

    /**
     * Initializes the optional value {@link TypedObjectValue#onMissing() onMissing} to onMissing.
     * @param onMissing The value for onMissing
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder onMissing(Optional<? extends Object> onMissing) {
      this.onMissing = onMissing.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableTypedObjectValue ImmutableTypedObjectValue}.
     * @return An immutable instance of TypedObjectValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTypedObjectValue build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableTypedObjectValue.validate(new ImmutableTypedObjectValue(this));
    }

    private boolean allowMissingIsSet() {
      return (optBits & OPT_BIT_ALLOW_MISSING) != 0;
    }

    private boolean allowUnknownTypesIsSet() {
      return (optBits & OPT_BIT_ALLOW_UNKNOWN_TYPES) != 0;
    }

    private boolean allowedTypesIsSet() {
      return (optBits & OPT_BIT_ALLOWED_TYPES) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TYPE_FIELD) != 0) attributes.add("typeField");
      return "Cannot build TypedObjectValue, some of required attributes are not set " + attributes;
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

  private static <K, V> Map<K, V> createUnmodifiableMap(boolean checkNulls, boolean skipNulls, Map<? extends K, ? extends V> map) {
    switch (map.size()) {
    case 0: return Collections.emptyMap();
    case 1: {
      Map.Entry<? extends K, ? extends V> e = map.entrySet().iterator().next();
      K k = e.getKey();
      V v = e.getValue();
      if (checkNulls) {
        Objects.requireNonNull(k, "key");
        if (v == null) Objects.requireNonNull(v, "value for key: " + k);
      }
      if (skipNulls && (k == null || v == null)) {
        return Collections.emptyMap();
      }
      return Collections.singletonMap(k, v);
    }
    default: {
      Map<K, V> linkedMap = new LinkedHashMap<>(map.size());
      if (skipNulls || checkNulls) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
          K k = e.getKey();
          V v = e.getValue();
          if (skipNulls) {
            if (k == null || v == null) continue;
          } else if (checkNulls) {
            Objects.requireNonNull(k, "key");
            if (v == null) Objects.requireNonNull(v, "value for key: " + k);
          }
          linkedMap.put(k, v);
        }
      } else {
        linkedMap.putAll(map);
      }
      return Collections.unmodifiableMap(linkedMap);
    }
    }
  }
}
