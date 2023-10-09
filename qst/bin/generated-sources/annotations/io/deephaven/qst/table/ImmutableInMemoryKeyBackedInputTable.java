package io.deephaven.qst.table;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link InMemoryKeyBackedInputTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableInMemoryKeyBackedInputTable.builder()}.
 */
@Generated(from = "InMemoryKeyBackedInputTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableInMemoryKeyBackedInputTable
    extends InMemoryKeyBackedInputTable {
  private transient final int depth;
  private final TableSchema schema;
  private final List<String> keys;
  private final UUID id;
  private transient final int hashCode;

  private ImmutableInMemoryKeyBackedInputTable(ImmutableInMemoryKeyBackedInputTable.Builder builder) {
    this.schema = builder.schema;
    this.keys = createUnmodifiableList(true, builder.keys);
    if (builder.idIsSet()) {
      initShim.id(builder.id);
    }
    this.depth = initShim.depth();
    this.id = initShim.id();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private ImmutableInMemoryKeyBackedInputTable(TableSchema schema, List<String> keys, UUID id) {
    this.schema = schema;
    this.keys = keys;
    initShim.id(id);
    this.depth = initShim.depth();
    this.id = initShim.id();
    this.hashCode = computeHashCode();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "InMemoryKeyBackedInputTable", generator = "Immutables")
  private final class InitShim {
    private byte depthBuildStage = STAGE_UNINITIALIZED;
    private int depth;

    int depth() {
      if (depthBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (depthBuildStage == STAGE_UNINITIALIZED) {
        depthBuildStage = STAGE_INITIALIZING;
        this.depth = ImmutableInMemoryKeyBackedInputTable.super.depth();
        depthBuildStage = STAGE_INITIALIZED;
      }
      return this.depth;
    }

    private byte idBuildStage = STAGE_UNINITIALIZED;
    private UUID id;

    UUID id() {
      if (idBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (idBuildStage == STAGE_UNINITIALIZED) {
        idBuildStage = STAGE_INITIALIZING;
        this.id = Objects.requireNonNull(ImmutableInMemoryKeyBackedInputTable.super.id(), "id");
        idBuildStage = STAGE_INITIALIZED;
      }
      return this.id;
    }

    void id(UUID id) {
      this.id = id;
      idBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (depthBuildStage == STAGE_INITIALIZING) attributes.add("depth");
      if (idBuildStage == STAGE_INITIALIZING) attributes.add("id");
      return "Cannot build InMemoryKeyBackedInputTable, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.depth()
        : this.depth;
  }

  /**
   * @return The value of the {@code schema} attribute
   */
  @Override
  public TableSchema schema() {
    return schema;
  }

  /**
   * @return The value of the {@code keys} attribute
   */
  @Override
  public List<String> keys() {
    return keys;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  UUID id() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.id()
        : this.id;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InMemoryKeyBackedInputTable#schema() schema} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schema
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInMemoryKeyBackedInputTable withSchema(TableSchema value) {
    if (this.schema == value) return this;
    TableSchema newValue = Objects.requireNonNull(value, "schema");
    return validate(new ImmutableInMemoryKeyBackedInputTable(newValue, this.keys, this.id));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InMemoryKeyBackedInputTable#keys() keys}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInMemoryKeyBackedInputTable withKeys(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableInMemoryKeyBackedInputTable(this.schema, newValue, this.id));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link InMemoryKeyBackedInputTable#keys() keys}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of keys elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableInMemoryKeyBackedInputTable withKeys(Iterable<String> elements) {
    if (this.keys == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableInMemoryKeyBackedInputTable(this.schema, newValue, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InMemoryKeyBackedInputTable#id() id} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInMemoryKeyBackedInputTable withId(UUID value) {
    if (this.id == value) return this;
    UUID newValue = Objects.requireNonNull(value, "id");
    return validate(new ImmutableInMemoryKeyBackedInputTable(this.schema, this.keys, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableInMemoryKeyBackedInputTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableInMemoryKeyBackedInputTable
        && equalTo(0, (ImmutableInMemoryKeyBackedInputTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableInMemoryKeyBackedInputTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && schema.equals(another.schema)
        && keys.equals(another.keys)
        && id.equals(another.id);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code schema}, {@code keys}, {@code id}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    int h = 5381;
    h += (h << 5) + getClass().hashCode();
    h += (h << 5) + depth;
    h += (h << 5) + schema.hashCode();
    h += (h << 5) + keys.hashCode();
    h += (h << 5) + id.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableInMemoryKeyBackedInputTable, WeakReference<ImmutableInMemoryKeyBackedInputTable>> INTERNER =
        new WeakHashMap<>();
  }

  private static ImmutableInMemoryKeyBackedInputTable validate(ImmutableInMemoryKeyBackedInputTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableInMemoryKeyBackedInputTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableInMemoryKeyBackedInputTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link InMemoryKeyBackedInputTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable InMemoryKeyBackedInputTable instance
   */
  public static ImmutableInMemoryKeyBackedInputTable copyOf(InMemoryKeyBackedInputTable instance) {
    if (instance instanceof ImmutableInMemoryKeyBackedInputTable) {
      return (ImmutableInMemoryKeyBackedInputTable) instance;
    }
    return ImmutableInMemoryKeyBackedInputTable.builder()
        .schema(instance.schema())
        .addAllKeys(instance.keys())
        .id(instance.id())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableInMemoryKeyBackedInputTable ImmutableInMemoryKeyBackedInputTable}.
   * <pre>
   * ImmutableInMemoryKeyBackedInputTable.builder()
   *    .schema(io.deephaven.qst.table.TableSchema) // required {@link InMemoryKeyBackedInputTable#schema() schema}
   *    .addKeys|addAllKeys(String) // {@link InMemoryKeyBackedInputTable#keys() keys} elements
   *    .id(UUID) // optional {@link InMemoryKeyBackedInputTable#id() id}
   *    .build();
   * </pre>
   * @return A new ImmutableInMemoryKeyBackedInputTable builder
   */
  public static ImmutableInMemoryKeyBackedInputTable.Builder builder() {
    return new ImmutableInMemoryKeyBackedInputTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableInMemoryKeyBackedInputTable ImmutableInMemoryKeyBackedInputTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "InMemoryKeyBackedInputTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_SCHEMA = 0x1L;
    private static final long OPT_BIT_ID = 0x1L;
    private long initBits = 0x1L;
    private long optBits;

    private TableSchema schema;
    private final List<String> keys = new ArrayList<String>();
    private UUID id;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link InMemoryKeyBackedInputTable#schema() schema} attribute.
     * @param schema The value for schema 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder schema(TableSchema schema) {
      checkNotIsSet(schemaIsSet(), "schema");
      this.schema = Objects.requireNonNull(schema, "schema");
      initBits &= ~INIT_BIT_SCHEMA;
      return this;
    }

    /**
     * Adds one element to {@link InMemoryKeyBackedInputTable#keys() keys} list.
     * @param element A keys element
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addKeys(String element) {
      this.keys.add(Objects.requireNonNull(element, "keys element"));
      return this;
    }

    /**
     * Adds elements to {@link InMemoryKeyBackedInputTable#keys() keys} list.
     * @param elements An array of keys elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addKeys(String... elements) {
      for (String element : elements) {
        this.keys.add(Objects.requireNonNull(element, "keys element"));
      }
      return this;
    }


    /**
     * Adds elements to {@link InMemoryKeyBackedInputTable#keys() keys} list.
     * @param elements An iterable of keys elements
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder addAllKeys(Iterable<String> elements) {
      for (String element : elements) {
        this.keys.add(Objects.requireNonNull(element, "keys element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link InMemoryKeyBackedInputTable#id() id} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link InMemoryKeyBackedInputTable#id() id}.</em>
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder id(UUID id) {
      checkNotIsSet(idIsSet(), "id");
      this.id = Objects.requireNonNull(id, "id");
      optBits |= OPT_BIT_ID;
      return this;
    }

    /**
     * Builds a new {@link ImmutableInMemoryKeyBackedInputTable ImmutableInMemoryKeyBackedInputTable}.
     * @return An immutable instance of InMemoryKeyBackedInputTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableInMemoryKeyBackedInputTable build() {
      checkRequiredAttributes();
      return ImmutableInMemoryKeyBackedInputTable.validate(new ImmutableInMemoryKeyBackedInputTable(this));
    }

    private boolean idIsSet() {
      return (optBits & OPT_BIT_ID) != 0;
    }

    private boolean schemaIsSet() {
      return (initBits & INIT_BIT_SCHEMA) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of InMemoryKeyBackedInputTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!schemaIsSet()) attributes.add("schema");
      return "Cannot build InMemoryKeyBackedInputTable, some of required attributes are not set " + attributes;
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
