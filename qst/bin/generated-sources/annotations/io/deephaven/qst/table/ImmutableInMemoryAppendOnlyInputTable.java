package io.deephaven.qst.table;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link InMemoryAppendOnlyInputTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableInMemoryAppendOnlyInputTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableInMemoryAppendOnlyInputTable.of()}.
 */
@Generated(from = "InMemoryAppendOnlyInputTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableInMemoryAppendOnlyInputTable
    extends InMemoryAppendOnlyInputTable {
  private transient final int depth;
  private final TableSchema schema;
  private final UUID id;
  private transient final int hashCode;

  private ImmutableInMemoryAppendOnlyInputTable(TableSchema schema, UUID id) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.id = Objects.requireNonNull(id, "id");
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableInMemoryAppendOnlyInputTable(
      ImmutableInMemoryAppendOnlyInputTable original,
      TableSchema schema,
      UUID id) {
    this.schema = schema;
    this.id = id;
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  /**
   * @return The computed-at-construction value of the {@code depth} attribute
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code schema} attribute
   */
  @Override
  public TableSchema schema() {
    return schema;
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @Override
  UUID id() {
    return id;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InMemoryAppendOnlyInputTable#schema() schema} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schema
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInMemoryAppendOnlyInputTable withSchema(TableSchema value) {
    if (this.schema == value) return this;
    TableSchema newValue = Objects.requireNonNull(value, "schema");
    return validate(new ImmutableInMemoryAppendOnlyInputTable(this, newValue, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link InMemoryAppendOnlyInputTable#id() id} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableInMemoryAppendOnlyInputTable withId(UUID value) {
    if (this.id == value) return this;
    UUID newValue = Objects.requireNonNull(value, "id");
    return validate(new ImmutableInMemoryAppendOnlyInputTable(this, this.schema, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableInMemoryAppendOnlyInputTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableInMemoryAppendOnlyInputTable
        && equalTo(0, (ImmutableInMemoryAppendOnlyInputTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableInMemoryAppendOnlyInputTable another) {
    if (hashCode != another.hashCode) return false;
    return depth == another.depth
        && schema.equals(another.schema)
        && id.equals(another.id);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code depth}, {@code schema}, {@code id}.
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
    h += (h << 5) + id.hashCode();
    return h;
  }

  private static final class InternerHolder {
    static final Map<ImmutableInMemoryAppendOnlyInputTable, WeakReference<ImmutableInMemoryAppendOnlyInputTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code InMemoryAppendOnlyInputTable} instance.
   * @param schema The value for the {@code schema} attribute
   * @param id The value for the {@code id} attribute
   * @return An immutable InMemoryAppendOnlyInputTable instance
   */
  public static ImmutableInMemoryAppendOnlyInputTable of(TableSchema schema, UUID id) {
    return validate(new ImmutableInMemoryAppendOnlyInputTable(schema, id));
  }

  private static ImmutableInMemoryAppendOnlyInputTable validate(ImmutableInMemoryAppendOnlyInputTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableInMemoryAppendOnlyInputTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableInMemoryAppendOnlyInputTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link InMemoryAppendOnlyInputTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable InMemoryAppendOnlyInputTable instance
   */
  public static ImmutableInMemoryAppendOnlyInputTable copyOf(InMemoryAppendOnlyInputTable instance) {
    if (instance instanceof ImmutableInMemoryAppendOnlyInputTable) {
      return (ImmutableInMemoryAppendOnlyInputTable) instance;
    }
    return ImmutableInMemoryAppendOnlyInputTable.builder()
        .schema(instance.schema())
        .id(instance.id())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableInMemoryAppendOnlyInputTable ImmutableInMemoryAppendOnlyInputTable}.
   * <pre>
   * ImmutableInMemoryAppendOnlyInputTable.builder()
   *    .schema(io.deephaven.qst.table.TableSchema) // required {@link InMemoryAppendOnlyInputTable#schema() schema}
   *    .id(UUID) // required {@link InMemoryAppendOnlyInputTable#id() id}
   *    .build();
   * </pre>
   * @return A new ImmutableInMemoryAppendOnlyInputTable builder
   */
  public static ImmutableInMemoryAppendOnlyInputTable.Builder builder() {
    return new ImmutableInMemoryAppendOnlyInputTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableInMemoryAppendOnlyInputTable ImmutableInMemoryAppendOnlyInputTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "InMemoryAppendOnlyInputTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_SCHEMA = 0x1L;
    private static final long INIT_BIT_ID = 0x2L;
    private long initBits = 0x3L;

    private TableSchema schema;
    private UUID id;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link InMemoryAppendOnlyInputTable#schema() schema} attribute.
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
     * Initializes the value for the {@link InMemoryAppendOnlyInputTable#id() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder id(UUID id) {
      checkNotIsSet(idIsSet(), "id");
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Builds a new {@link ImmutableInMemoryAppendOnlyInputTable ImmutableInMemoryAppendOnlyInputTable}.
     * @return An immutable instance of InMemoryAppendOnlyInputTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableInMemoryAppendOnlyInputTable build() {
      checkRequiredAttributes();
      return ImmutableInMemoryAppendOnlyInputTable.validate(new ImmutableInMemoryAppendOnlyInputTable(null, schema, id));
    }

    private boolean schemaIsSet() {
      return (initBits & INIT_BIT_SCHEMA) == 0;
    }

    private boolean idIsSet() {
      return (initBits & INIT_BIT_ID) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of InMemoryAppendOnlyInputTable is strict, attribute is already set: ".concat(name));
    }

    private void checkRequiredAttributes() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if (!schemaIsSet()) attributes.add("schema");
      if (!idIsSet()) attributes.add("id");
      return "Cannot build InMemoryAppendOnlyInputTable, some of required attributes are not set " + attributes;
    }
  }
}
