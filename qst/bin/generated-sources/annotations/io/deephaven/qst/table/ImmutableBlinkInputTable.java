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
 * Immutable implementation of {@link BlinkInputTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableBlinkInputTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableBlinkInputTable.of()}.
 */
@Generated(from = "BlinkInputTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableBlinkInputTable extends BlinkInputTable {
  private transient final int depth;
  private final TableSchema schema;
  private final UUID id;
  private transient final int hashCode;

  private ImmutableBlinkInputTable(TableSchema schema, UUID id) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.id = Objects.requireNonNull(id, "id");
    this.depth = super.depth();
    this.hashCode = computeHashCode();
  }

  private ImmutableBlinkInputTable(ImmutableBlinkInputTable original, TableSchema schema, UUID id) {
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
   * Copy the current immutable object by setting a value for the {@link BlinkInputTable#schema() schema} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schema
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBlinkInputTable withSchema(TableSchema value) {
    if (this.schema == value) return this;
    TableSchema newValue = Objects.requireNonNull(value, "schema");
    return validate(new ImmutableBlinkInputTable(this, newValue, this.id));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link BlinkInputTable#id() id} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableBlinkInputTable withId(UUID value) {
    if (this.id == value) return this;
    UUID newValue = Objects.requireNonNull(value, "id");
    return validate(new ImmutableBlinkInputTable(this, this.schema, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableBlinkInputTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableBlinkInputTable
        && equalTo(0, (ImmutableBlinkInputTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableBlinkInputTable another) {
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
    static final Map<ImmutableBlinkInputTable, WeakReference<ImmutableBlinkInputTable>> INTERNER =
        new WeakHashMap<>();
  }

  /**
   * Construct a new immutable {@code BlinkInputTable} instance.
   * @param schema The value for the {@code schema} attribute
   * @param id The value for the {@code id} attribute
   * @return An immutable BlinkInputTable instance
   */
  public static ImmutableBlinkInputTable of(TableSchema schema, UUID id) {
    return validate(new ImmutableBlinkInputTable(schema, id));
  }

  private static ImmutableBlinkInputTable validate(ImmutableBlinkInputTable instance) {
    synchronized (InternerHolder.INTERNER) {
      WeakReference<ImmutableBlinkInputTable> reference = InternerHolder.INTERNER.get(instance);
      ImmutableBlinkInputTable interned = reference != null ? reference.get() : null;
      if (interned == null) {
        InternerHolder.INTERNER.put(instance, new WeakReference<>(instance));
        interned = instance;
      }
      return interned;
    }
  }

  /**
   * Creates an immutable copy of a {@link BlinkInputTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable BlinkInputTable instance
   */
  public static ImmutableBlinkInputTable copyOf(BlinkInputTable instance) {
    if (instance instanceof ImmutableBlinkInputTable) {
      return (ImmutableBlinkInputTable) instance;
    }
    return ImmutableBlinkInputTable.builder()
        .schema(instance.schema())
        .id(instance.id())
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableBlinkInputTable ImmutableBlinkInputTable}.
   * <pre>
   * ImmutableBlinkInputTable.builder()
   *    .schema(io.deephaven.qst.table.TableSchema) // required {@link BlinkInputTable#schema() schema}
   *    .id(UUID) // required {@link BlinkInputTable#id() id}
   *    .build();
   * </pre>
   * @return A new ImmutableBlinkInputTable builder
   */
  public static ImmutableBlinkInputTable.Builder builder() {
    return new ImmutableBlinkInputTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableBlinkInputTable ImmutableBlinkInputTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "BlinkInputTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_SCHEMA = 0x1L;
    private static final long INIT_BIT_ID = 0x2L;
    private long initBits = 0x3L;

    private TableSchema schema;
    private UUID id;

    private Builder() {
    }

    /**
     * Initializes the value for the {@link BlinkInputTable#schema() schema} attribute.
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
     * Initializes the value for the {@link BlinkInputTable#id() id} attribute.
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
     * Builds a new {@link ImmutableBlinkInputTable ImmutableBlinkInputTable}.
     * @return An immutable instance of BlinkInputTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableBlinkInputTable build() {
      checkRequiredAttributes();
      return ImmutableBlinkInputTable.validate(new ImmutableBlinkInputTable(null, schema, id));
    }

    private boolean schemaIsSet() {
      return (initBits & INIT_BIT_SCHEMA) == 0;
    }

    private boolean idIsSet() {
      return (initBits & INIT_BIT_ID) == 0;
    }

    private static void checkNotIsSet(boolean isSet, String name) {
      if (isSet) throw new IllegalStateException("Builder of BlinkInputTable is strict, attribute is already set: ".concat(name));
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
      return "Cannot build BlinkInputTable, some of required attributes are not set " + attributes;
    }
  }
}
