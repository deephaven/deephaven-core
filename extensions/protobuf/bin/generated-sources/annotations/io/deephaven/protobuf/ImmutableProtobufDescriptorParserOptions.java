package io.deephaven.protobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ProtobufDescriptorParserOptions}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableProtobufDescriptorParserOptions.builder()}.
 */
@Generated(from = "ProtobufDescriptorParserOptions", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableProtobufDescriptorParserOptions
    extends ProtobufDescriptorParserOptions {
  private final Function<FieldPath, FieldOptions> fieldOptions;
  private final List<MessageParser> parsers;

  private ImmutableProtobufDescriptorParserOptions(ImmutableProtobufDescriptorParserOptions.Builder builder) {
    if (builder.fieldOptions != null) {
      initShim.fieldOptions(builder.fieldOptions);
    }
    if (builder.parsersIsSet()) {
      initShim.parsers(createUnmodifiableList(true, builder.parsers));
    }
    this.fieldOptions = initShim.fieldOptions();
    this.parsers = initShim.parsers();
    this.initShim = null;
  }

  private ImmutableProtobufDescriptorParserOptions(
      Function<FieldPath, FieldOptions> fieldOptions,
      List<MessageParser> parsers) {
    this.fieldOptions = fieldOptions;
    this.parsers = parsers;
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "ProtobufDescriptorParserOptions", generator = "Immutables")
  private final class InitShim {
    private byte fieldOptionsBuildStage = STAGE_UNINITIALIZED;
    private Function<FieldPath, FieldOptions> fieldOptions;

    Function<FieldPath, FieldOptions> fieldOptions() {
      if (fieldOptionsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (fieldOptionsBuildStage == STAGE_UNINITIALIZED) {
        fieldOptionsBuildStage = STAGE_INITIALIZING;
        this.fieldOptions = Objects.requireNonNull(ImmutableProtobufDescriptorParserOptions.super.fieldOptions(), "fieldOptions");
        fieldOptionsBuildStage = STAGE_INITIALIZED;
      }
      return this.fieldOptions;
    }

    void fieldOptions(Function<FieldPath, FieldOptions> fieldOptions) {
      this.fieldOptions = fieldOptions;
      fieldOptionsBuildStage = STAGE_INITIALIZED;
    }

    private byte parsersBuildStage = STAGE_UNINITIALIZED;
    private List<MessageParser> parsers;

    List<MessageParser> parsers() {
      if (parsersBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (parsersBuildStage == STAGE_UNINITIALIZED) {
        parsersBuildStage = STAGE_INITIALIZING;
        this.parsers = createUnmodifiableList(false, createSafeList(ImmutableProtobufDescriptorParserOptions.super.parsers(), true, false));
        parsersBuildStage = STAGE_INITIALIZED;
      }
      return this.parsers;
    }

    void parsers(List<MessageParser> parsers) {
      this.parsers = parsers;
      parsersBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (fieldOptionsBuildStage == STAGE_INITIALIZING) attributes.add("fieldOptions");
      if (parsersBuildStage == STAGE_INITIALIZING) attributes.add("parsers");
      return "Cannot build ProtobufDescriptorParserOptions, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * The field options, allows the caller to specify different options for different field paths. Equivalent to
   * {@code fieldPath -> FieldOptions.defaults()}.
   * @return the field options
   */
  @Override
  public Function<FieldPath, FieldOptions> fieldOptions() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.fieldOptions()
        : this.fieldOptions;
  }

  /**
   * Controls which message parsers to use. By default, is {@link MessageParser#defaults()}.
   * 
   * @return the single-valued message parsers
   */
  @Override
  public List<MessageParser> parsers() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.parsers()
        : this.parsers;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ProtobufDescriptorParserOptions#fieldOptions() fieldOptions} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fieldOptions
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableProtobufDescriptorParserOptions withFieldOptions(Function<FieldPath, FieldOptions> value) {
    if (this.fieldOptions == value) return this;
    Function<FieldPath, FieldOptions> newValue = Objects.requireNonNull(value, "fieldOptions");
    return new ImmutableProtobufDescriptorParserOptions(newValue, this.parsers);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ProtobufDescriptorParserOptions#parsers() parsers}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtobufDescriptorParserOptions withParsers(MessageParser... elements) {
    List<MessageParser> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableProtobufDescriptorParserOptions(this.fieldOptions, newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ProtobufDescriptorParserOptions#parsers() parsers}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of parsers elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableProtobufDescriptorParserOptions withParsers(Iterable<? extends MessageParser> elements) {
    if (this.parsers == elements) return this;
    List<MessageParser> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableProtobufDescriptorParserOptions(this.fieldOptions, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableProtobufDescriptorParserOptions} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableProtobufDescriptorParserOptions
        && equalTo(0, (ImmutableProtobufDescriptorParserOptions) another);
  }

  private boolean equalTo(int synthetic, ImmutableProtobufDescriptorParserOptions another) {
    return fieldOptions.equals(another.fieldOptions)
        && parsers.equals(another.parsers);
  }

  /**
   * Computes a hash code from attributes: {@code fieldOptions}, {@code parsers}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + fieldOptions.hashCode();
    h += (h << 5) + parsers.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ProtobufDescriptorParserOptions} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ProtobufDescriptorParserOptions{"
        + "fieldOptions=" + fieldOptions
        + ", parsers=" + parsers
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ProtobufDescriptorParserOptions} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ProtobufDescriptorParserOptions instance
   */
  public static ImmutableProtobufDescriptorParserOptions copyOf(ProtobufDescriptorParserOptions instance) {
    if (instance instanceof ImmutableProtobufDescriptorParserOptions) {
      return (ImmutableProtobufDescriptorParserOptions) instance;
    }
    return ImmutableProtobufDescriptorParserOptions.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableProtobufDescriptorParserOptions ImmutableProtobufDescriptorParserOptions}.
   * <pre>
   * ImmutableProtobufDescriptorParserOptions.builder()
   *    .fieldOptions(function.Function&amp;lt;io.deephaven.protobuf.FieldPath, io.deephaven.protobuf.FieldOptions&amp;gt;) // optional {@link ProtobufDescriptorParserOptions#fieldOptions() fieldOptions}
   *    .addParsers|addAllParsers(io.deephaven.protobuf.MessageParser) // {@link ProtobufDescriptorParserOptions#parsers() parsers} elements
   *    .build();
   * </pre>
   * @return A new ImmutableProtobufDescriptorParserOptions builder
   */
  public static ImmutableProtobufDescriptorParserOptions.Builder builder() {
    return new ImmutableProtobufDescriptorParserOptions.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableProtobufDescriptorParserOptions ImmutableProtobufDescriptorParserOptions}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ProtobufDescriptorParserOptions", generator = "Immutables")
  public static final class Builder implements ProtobufDescriptorParserOptions.Builder {
    private static final long OPT_BIT_PARSERS = 0x1L;
    private long optBits;

    private Function<FieldPath, FieldOptions> fieldOptions;
    private List<MessageParser> parsers = new ArrayList<MessageParser>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ProtobufDescriptorParserOptions} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ProtobufDescriptorParserOptions instance) {
      Objects.requireNonNull(instance, "instance");
      fieldOptions(instance.fieldOptions());
      addAllParsers(instance.parsers());
      return this;
    }

    /**
     * Initializes the value for the {@link ProtobufDescriptorParserOptions#fieldOptions() fieldOptions} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ProtobufDescriptorParserOptions#fieldOptions() fieldOptions}.</em>
     * @param fieldOptions The value for fieldOptions 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder fieldOptions(Function<FieldPath, FieldOptions> fieldOptions) {
      this.fieldOptions = Objects.requireNonNull(fieldOptions, "fieldOptions");
      return this;
    }

    /**
     * Adds one element to {@link ProtobufDescriptorParserOptions#parsers() parsers} list.
     * @param element A parsers element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addParsers(MessageParser element) {
      this.parsers.add(Objects.requireNonNull(element, "parsers element"));
      optBits |= OPT_BIT_PARSERS;
      return this;
    }

    /**
     * Adds elements to {@link ProtobufDescriptorParserOptions#parsers() parsers} list.
     * @param elements An array of parsers elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addParsers(MessageParser... elements) {
      for (MessageParser element : elements) {
        this.parsers.add(Objects.requireNonNull(element, "parsers element"));
      }
      optBits |= OPT_BIT_PARSERS;
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ProtobufDescriptorParserOptions#parsers() parsers} list.
     * @param elements An iterable of parsers elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parsers(Iterable<? extends MessageParser> elements) {
      this.parsers.clear();
      return addAllParsers(elements);
    }

    /**
     * Adds elements to {@link ProtobufDescriptorParserOptions#parsers() parsers} list.
     * @param elements An iterable of parsers elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllParsers(Iterable<? extends MessageParser> elements) {
      for (MessageParser element : elements) {
        this.parsers.add(Objects.requireNonNull(element, "parsers element"));
      }
      optBits |= OPT_BIT_PARSERS;
      return this;
    }

    /**
     * Builds a new {@link ImmutableProtobufDescriptorParserOptions ImmutableProtobufDescriptorParserOptions}.
     * @return An immutable instance of ProtobufDescriptorParserOptions
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableProtobufDescriptorParserOptions build() {
      return new ImmutableProtobufDescriptorParserOptions(this);
    }

    private boolean parsersIsSet() {
      return (optBits & OPT_BIT_PARSERS) != 0;
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

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
    }
  }
}
