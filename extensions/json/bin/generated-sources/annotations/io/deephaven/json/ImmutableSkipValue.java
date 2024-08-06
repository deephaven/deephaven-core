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
 * Immutable implementation of {@link SkipValue}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSkipValue.builder()}.
 */
@Generated(from = "SkipValue", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableSkipValue extends SkipValue {
  private final boolean allowMissing;
  private final Set<JsonValueTypes> allowedTypes;

  private ImmutableSkipValue(ImmutableSkipValue.Builder builder) {
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

  private ImmutableSkipValue(boolean allowMissing, Set<JsonValueTypes> allowedTypes) {
    this.allowMissing = allowMissing;
    this.allowedTypes = allowedTypes;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "SkipValue", generator = "Immutables")
  private final class InitShim {
    private byte allowMissingBuildStage = STAGE_UNINITIALIZED;
    private boolean allowMissing;

    boolean allowMissing() {
      if (allowMissingBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (allowMissingBuildStage == STAGE_UNINITIALIZED) {
        allowMissingBuildStage = STAGE_INITIALIZING;
        this.allowMissing = ImmutableSkipValue.super.allowMissing();
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
        this.allowedTypes = createUnmodifiableEnumSet(ImmutableSkipValue.super.allowedTypes());
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
      return "Cannot build SkipValue, attribute initializers form cycle " + attributes;
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
   * {@inheritDoc} By default is {@link JsonValueTypes#all()}.
   */
  @Override
  public Set<JsonValueTypes> allowedTypes() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.allowedTypes()
        : this.allowedTypes;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SkipValue#allowMissing() allowMissing} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for allowMissing
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSkipValue withAllowMissing(boolean value) {
    if (this.allowMissing == value) return this;
    return validate(new ImmutableSkipValue(value, this.allowedTypes));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SkipValue#allowedTypes() allowedTypes}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSkipValue withAllowedTypes(JsonValueTypes... elements) {
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(Arrays.asList(elements));
    return validate(new ImmutableSkipValue(this.allowMissing, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SkipValue#allowedTypes() allowedTypes}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of allowedTypes elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSkipValue withAllowedTypes(Iterable<JsonValueTypes> elements) {
    if (this.allowedTypes == elements) return this;
    Set<JsonValueTypes> newValue = createUnmodifiableEnumSet(elements);
    return validate(new ImmutableSkipValue(this.allowMissing, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSkipValue} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSkipValue
        && equalTo(0, (ImmutableSkipValue) another);
  }

  private boolean equalTo(int synthetic, ImmutableSkipValue another) {
    return allowMissing == another.allowMissing
        && allowedTypes.equals(another.allowedTypes);
  }

  /**
   * Computes a hash code from attributes: {@code allowMissing}, {@code allowedTypes}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + Boolean.hashCode(allowMissing);
    h += (h << 5) + allowedTypes.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SkipValue} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SkipValue{"
        + "allowMissing=" + allowMissing
        + ", allowedTypes=" + allowedTypes
        + "}";
  }

  private static ImmutableSkipValue validate(ImmutableSkipValue instance) {
    instance.checkAllowedTypeInvariants();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link SkipValue} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SkipValue instance
   */
  public static ImmutableSkipValue copyOf(SkipValue instance) {
    if (instance instanceof ImmutableSkipValue) {
      return (ImmutableSkipValue) instance;
    }
    return ImmutableSkipValue.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSkipValue ImmutableSkipValue}.
   * <pre>
   * ImmutableSkipValue.builder()
   *    .allowMissing(boolean) // optional {@link SkipValue#allowMissing() allowMissing}
   *    .addAllowedTypes|addAllAllowedTypes(io.deephaven.json.JsonValueTypes) // {@link SkipValue#allowedTypes() allowedTypes} elements
   *    .build();
   * </pre>
   * @return A new ImmutableSkipValue builder
   */
  public static ImmutableSkipValue.Builder builder() {
    return new ImmutableSkipValue.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSkipValue ImmutableSkipValue}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SkipValue", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements SkipValue.Builder {
    private static final long OPT_BIT_ALLOW_MISSING = 0x1L;
    private static final long OPT_BIT_ALLOWED_TYPES = 0x2L;
    private long optBits;

    private boolean allowMissing;
    private EnumSet<JsonValueTypes> allowedTypes = EnumSet.noneOf(JsonValueTypes.class);

    private Builder() {
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
     * Fill a builder with attribute values from the provided {@code io.deephaven.json.SkipValue} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SkipValue instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      if (object instanceof Value) {
        Value instance = (Value) object;
        addAllAllowedTypes(instance.allowedTypes());
        allowMissing(instance.allowMissing());
      }
    }

    /**
     * Initializes the value for the {@link SkipValue#allowMissing() allowMissing} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link SkipValue#allowMissing() allowMissing}.</em>
     * @param allowMissing The value for allowMissing 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowMissing(boolean allowMissing) {
      this.allowMissing = allowMissing;
      optBits |= OPT_BIT_ALLOW_MISSING;
      return this;
    }

    /**
     * Adds one element to {@link SkipValue#allowedTypes() allowedTypes} set.
     * @param element A allowedTypes element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllowedTypes(JsonValueTypes element) {
      this.allowedTypes.add(Objects.requireNonNull(element, "allowedTypes element"));
      optBits |= OPT_BIT_ALLOWED_TYPES;
      return this;
    }

    /**
     * Adds elements to {@link SkipValue#allowedTypes() allowedTypes} set.
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
     * Sets or replaces all elements for {@link SkipValue#allowedTypes() allowedTypes} set.
     * @param elements An iterable of allowedTypes elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder allowedTypes(Iterable<JsonValueTypes> elements) {
      this.allowedTypes.clear();
      return addAllAllowedTypes(elements);
    }

    /**
     * Adds elements to {@link SkipValue#allowedTypes() allowedTypes} set.
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
     * Builds a new {@link ImmutableSkipValue ImmutableSkipValue}.
     * @return An immutable instance of SkipValue
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSkipValue build() {
      return ImmutableSkipValue.validate(new ImmutableSkipValue(this));
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
