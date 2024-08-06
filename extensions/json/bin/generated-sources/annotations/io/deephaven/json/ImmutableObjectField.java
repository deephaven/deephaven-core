package io.deephaven.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
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
 * Immutable implementation of {@link ObjectField}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableObjectField.builder()}.
 */
@Generated(from = "ObjectField", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
public final class ImmutableObjectField extends ObjectField {
  private final String name;
  private final Value options;
  private final Set<String> aliases;
  private final boolean caseSensitive;
  private final ObjectField.RepeatedBehavior repeatedBehavior;
  private final @Nullable Object arrayGroup;

  private ImmutableObjectField(ImmutableObjectField.Builder builder) {
    this.name = builder.name;
    this.options = builder.options;
    this.aliases = createUnmodifiableSet(builder.aliases);
    this.arrayGroup = builder.arrayGroup;
    if (builder.caseSensitiveIsSet()) {
      initShim.caseSensitive(builder.caseSensitive);
    }
    if (builder.repeatedBehavior != null) {
      initShim.repeatedBehavior(builder.repeatedBehavior);
    }
    this.caseSensitive = initShim.caseSensitive();
    this.repeatedBehavior = initShim.repeatedBehavior();
    this.initShim = null;
  }

  private ImmutableObjectField(
      String name,
      Value options,
      Set<String> aliases,
      boolean caseSensitive,
      ObjectField.RepeatedBehavior repeatedBehavior,
      @Nullable Object arrayGroup) {
    this.name = name;
    this.options = options;
    this.aliases = aliases;
    this.caseSensitive = caseSensitive;
    this.repeatedBehavior = repeatedBehavior;
    this.arrayGroup = arrayGroup;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ObjectField", generator = "Immutables")
  private final class InitShim {
    private byte caseSensitiveBuildStage = STAGE_UNINITIALIZED;
    private boolean caseSensitive;

    boolean caseSensitive() {
      if (caseSensitiveBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (caseSensitiveBuildStage == STAGE_UNINITIALIZED) {
        caseSensitiveBuildStage = STAGE_INITIALIZING;
        this.caseSensitive = ImmutableObjectField.super.caseSensitive();
        caseSensitiveBuildStage = STAGE_INITIALIZED;
      }
      return this.caseSensitive;
    }

    void caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      caseSensitiveBuildStage = STAGE_INITIALIZED;
    }

    private byte repeatedBehaviorBuildStage = STAGE_UNINITIALIZED;
    private ObjectField.RepeatedBehavior repeatedBehavior;

    ObjectField.RepeatedBehavior repeatedBehavior() {
      if (repeatedBehaviorBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (repeatedBehaviorBuildStage == STAGE_UNINITIALIZED) {
        repeatedBehaviorBuildStage = STAGE_INITIALIZING;
        this.repeatedBehavior = Objects.requireNonNull(ImmutableObjectField.super.repeatedBehavior(), "repeatedBehavior");
        repeatedBehaviorBuildStage = STAGE_INITIALIZED;
      }
      return this.repeatedBehavior;
    }

    void repeatedBehavior(ObjectField.RepeatedBehavior repeatedBehavior) {
      this.repeatedBehavior = repeatedBehavior;
      repeatedBehaviorBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (caseSensitiveBuildStage == STAGE_INITIALIZING) attributes.add("caseSensitive");
      if (repeatedBehaviorBuildStage == STAGE_INITIALIZING) attributes.add("repeatedBehavior");
      return "Cannot build ObjectField, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The canonical field name.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * The value options.
   */
  @Override
  public Value options() {
    return options;
  }

  /**
   * The field name aliases.
   */
  @Override
  public Set<String> aliases() {
    return aliases;
  }

  /**
   * If the field name and aliases should be compared using case-sensitive equality. By default is {@code true}.
   */
  @Override
  public boolean caseSensitive() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.caseSensitive()
        : this.caseSensitive;
  }

  /**
   * The behavior when a repeated field is encountered. By default is {@link RepeatedBehavior#ERROR}.
   */
  @Override
  public ObjectField.RepeatedBehavior repeatedBehavior() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.repeatedBehavior()
        : this.repeatedBehavior;
  }

  /**
   * The array group for {@code this} field. This is useful in scenarios where {@code this} field's array is
   * guaranteed to have the same cardinality as one or more other array fields. For example, in the following snippet,
   * we might model "prices" and "quantities" as having the same array group:
   * <pre>
   * {
   *   "prices": [1.1, 2.2, 3.3],
   *   "quantities": [9, 5, 42]
   * }
   * </pre>
   */
  @Override
  public Optional<Object> arrayGroup() {
    return Optional.ofNullable(arrayGroup);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectField#name() name} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for name
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectField withName(String value) {
    String newValue = Objects.requireNonNull(value, "name");
    if (this.name.equals(newValue)) return this;
    return validate(new ImmutableObjectField(
        newValue,
        this.options,
        this.aliases,
        this.caseSensitive,
        this.repeatedBehavior,
        this.arrayGroup));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectField#options() options} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for options
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectField withOptions(Value value) {
    if (this.options == value) return this;
    Value newValue = Objects.requireNonNull(value, "options");
    return validate(new ImmutableObjectField(this.name, newValue, this.aliases, this.caseSensitive, this.repeatedBehavior, this.arrayGroup));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectField#aliases() aliases}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectField withAliases(String... elements) {
    Set<String> newValue = createUnmodifiableSet(createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableObjectField(this.name, this.options, newValue, this.caseSensitive, this.repeatedBehavior, this.arrayGroup));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ObjectField#aliases() aliases}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of aliases elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectField withAliases(Iterable<String> elements) {
    if (this.aliases == elements) return this;
    Set<String> newValue = createUnmodifiableSet(createSafeList(elements, true, false));
    return validate(new ImmutableObjectField(this.name, this.options, newValue, this.caseSensitive, this.repeatedBehavior, this.arrayGroup));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectField#caseSensitive() caseSensitive} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for caseSensitive
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectField withCaseSensitive(boolean value) {
    if (this.caseSensitive == value) return this;
    return validate(new ImmutableObjectField(this.name, this.options, this.aliases, value, this.repeatedBehavior, this.arrayGroup));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ObjectField#repeatedBehavior() repeatedBehavior} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for repeatedBehavior
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableObjectField withRepeatedBehavior(ObjectField.RepeatedBehavior value) {
    ObjectField.RepeatedBehavior newValue = Objects.requireNonNull(value, "repeatedBehavior");
    if (this.repeatedBehavior == newValue) return this;
    return validate(new ImmutableObjectField(this.name, this.options, this.aliases, this.caseSensitive, newValue, this.arrayGroup));
  }

  /**
   * Copy the current immutable object by setting a <i>present</i> value for the optional {@link ObjectField#arrayGroup() arrayGroup} attribute.
   * @param value The value for arrayGroup
   * @return A modified copy of {@code this} object
   */
  public final ImmutableObjectField withArrayGroup(Object value) {
    @Nullable Object newValue = Objects.requireNonNull(value, "arrayGroup");
    if (this.arrayGroup == newValue) return this;
    return validate(new ImmutableObjectField(this.name, this.options, this.aliases, this.caseSensitive, this.repeatedBehavior, newValue));
  }

  /**
   * Copy the current immutable object by setting an optional value for the {@link ObjectField#arrayGroup() arrayGroup} attribute.
   * A shallow reference equality check is used on unboxed optional value to prevent copying of the same value by returning {@code this}.
   * @param optional A value for arrayGroup
   * @return A modified copy of {@code this} object
   */
  @SuppressWarnings("unchecked") // safe covariant cast
  public final ImmutableObjectField withArrayGroup(Optional<? extends Object> optional) {
    @Nullable Object value = optional.orElse(null);
    if (this.arrayGroup == value) return this;
    return validate(new ImmutableObjectField(this.name, this.options, this.aliases, this.caseSensitive, this.repeatedBehavior, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableObjectField} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableObjectField
        && equalTo(0, (ImmutableObjectField) another);
  }

  private boolean equalTo(int synthetic, ImmutableObjectField another) {
    return name.equals(another.name)
        && options.equals(another.options)
        && aliases.equals(another.aliases)
        && caseSensitive == another.caseSensitive
        && repeatedBehavior.equals(another.repeatedBehavior)
        && Objects.equals(arrayGroup, another.arrayGroup);
  }

  /**
   * Computes a hash code from attributes: {@code name}, {@code options}, {@code aliases}, {@code caseSensitive}, {@code repeatedBehavior}, {@code arrayGroup}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + name.hashCode();
    h += (h << 5) + options.hashCode();
    h += (h << 5) + aliases.hashCode();
    h += (h << 5) + Boolean.hashCode(caseSensitive);
    h += (h << 5) + repeatedBehavior.hashCode();
    h += (h << 5) + Objects.hashCode(arrayGroup);
    return h;
  }

  /**
   * Prints the immutable value {@code ObjectField} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("ObjectField{");
    builder.append("name=").append(name);
    builder.append(", ");
    builder.append("options=").append(options);
    builder.append(", ");
    builder.append("aliases=").append(aliases);
    builder.append(", ");
    builder.append("caseSensitive=").append(caseSensitive);
    builder.append(", ");
    builder.append("repeatedBehavior=").append(repeatedBehavior);
    if (arrayGroup != null) {
      builder.append(", ");
      builder.append("arrayGroup=").append(arrayGroup);
    }
    return builder.append("}").toString();
  }

  private static ImmutableObjectField validate(ImmutableObjectField instance) {
    instance.checkArrayGroup();
    instance.checkNonOverlapping();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ObjectField} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ObjectField instance
   */
  public static ImmutableObjectField copyOf(ObjectField instance) {
    if (instance instanceof ImmutableObjectField) {
      return (ImmutableObjectField) instance;
    }
    return ImmutableObjectField.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableObjectField ImmutableObjectField}.
   * <pre>
   * ImmutableObjectField.builder()
   *    .name(String) // required {@link ObjectField#name() name}
   *    .options(io.deephaven.json.Value) // required {@link ObjectField#options() options}
   *    .addAliases|addAllAliases(String) // {@link ObjectField#aliases() aliases} elements
   *    .caseSensitive(boolean) // optional {@link ObjectField#caseSensitive() caseSensitive}
   *    .repeatedBehavior(io.deephaven.json.ObjectField.RepeatedBehavior) // optional {@link ObjectField#repeatedBehavior() repeatedBehavior}
   *    .arrayGroup(Object) // optional {@link ObjectField#arrayGroup() arrayGroup}
   *    .build();
   * </pre>
   * @return A new ImmutableObjectField builder
   */
  public static ImmutableObjectField.Builder builder() {
    return new ImmutableObjectField.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableObjectField ImmutableObjectField}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ObjectField", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder implements ObjectField.Builder {
    private static final long INIT_BIT_NAME = 0x1L;
    private static final long INIT_BIT_OPTIONS = 0x2L;
    private static final long OPT_BIT_CASE_SENSITIVE = 0x1L;
    private long initBits = 0x3L;
    private long optBits;

    private @Nullable String name;
    private @Nullable Value options;
    private List<String> aliases = new ArrayList<String>();
    private boolean caseSensitive;
    private @Nullable ObjectField.RepeatedBehavior repeatedBehavior;
    private @Nullable Object arrayGroup;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ObjectField} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ObjectField instance) {
      Objects.requireNonNull(instance, "instance");
      name(instance.name());
      options(instance.options());
      addAllAliases(instance.aliases());
      caseSensitive(instance.caseSensitive());
      repeatedBehavior(instance.repeatedBehavior());
      Optional<Object> arrayGroupOptional = instance.arrayGroup();
      if (arrayGroupOptional.isPresent()) {
        arrayGroup(arrayGroupOptional);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectField#name() name} attribute.
     * @param name The value for name 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder name(String name) {
      this.name = Objects.requireNonNull(name, "name");
      initBits &= ~INIT_BIT_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectField#options() options} attribute.
     * @param options The value for options 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder options(Value options) {
      this.options = Objects.requireNonNull(options, "options");
      initBits &= ~INIT_BIT_OPTIONS;
      return this;
    }

    /**
     * Adds one element to {@link ObjectField#aliases() aliases} set.
     * @param element A aliases element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAliases(String element) {
      this.aliases.add(Objects.requireNonNull(element, "aliases element"));
      return this;
    }

    /**
     * Adds elements to {@link ObjectField#aliases() aliases} set.
     * @param elements An array of aliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAliases(String... elements) {
      for (String element : elements) {
        this.aliases.add(Objects.requireNonNull(element, "aliases element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ObjectField#aliases() aliases} set.
     * @param elements An iterable of aliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder aliases(Iterable<String> elements) {
      this.aliases.clear();
      return addAllAliases(elements);
    }

    /**
     * Adds elements to {@link ObjectField#aliases() aliases} set.
     * @param elements An iterable of aliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllAliases(Iterable<String> elements) {
      for (String element : elements) {
        this.aliases.add(Objects.requireNonNull(element, "aliases element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectField#caseSensitive() caseSensitive} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectField#caseSensitive() caseSensitive}.</em>
     * @param caseSensitive The value for caseSensitive 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      optBits |= OPT_BIT_CASE_SENSITIVE;
      return this;
    }

    /**
     * Initializes the value for the {@link ObjectField#repeatedBehavior() repeatedBehavior} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ObjectField#repeatedBehavior() repeatedBehavior}.</em>
     * @param repeatedBehavior The value for repeatedBehavior 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder repeatedBehavior(ObjectField.RepeatedBehavior repeatedBehavior) {
      this.repeatedBehavior = Objects.requireNonNull(repeatedBehavior, "repeatedBehavior");
      return this;
    }

    /**
     * Initializes the optional value {@link ObjectField#arrayGroup() arrayGroup} to arrayGroup.
     * @param arrayGroup The value for arrayGroup
     * @return {@code this} builder for chained invocation
     */
    public final Builder arrayGroup(Object arrayGroup) {
      this.arrayGroup = Objects.requireNonNull(arrayGroup, "arrayGroup");
      return this;
    }

    /**
     * Initializes the optional value {@link ObjectField#arrayGroup() arrayGroup} to arrayGroup.
     * @param arrayGroup The value for arrayGroup
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder arrayGroup(Optional<? extends Object> arrayGroup) {
      this.arrayGroup = arrayGroup.orElse(null);
      return this;
    }

    /**
     * Builds a new {@link ImmutableObjectField ImmutableObjectField}.
     * @return An immutable instance of ObjectField
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableObjectField build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableObjectField.validate(new ImmutableObjectField(this));
    }

    private boolean caseSensitiveIsSet() {
      return (optBits & OPT_BIT_CASE_SENSITIVE) != 0;
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_NAME) != 0) attributes.add("name");
      if ((initBits & INIT_BIT_OPTIONS) != 0) attributes.add("options");
      return "Cannot build ObjectField, some of required attributes are not set " + attributes;
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
}
