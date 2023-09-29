package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ReverseTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableReverseTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableReverseTable.of()}.
 */
@Generated(from = "ReverseTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableReverseTable extends ReverseTable {
  private transient final int depth;
  private final TableSpec parent;

  private ImmutableReverseTable(TableSpec parent) {
    this.parent = Objects.requireNonNull(parent, "parent");
    this.depth = super.depth();
  }

  private ImmutableReverseTable(ImmutableReverseTable original, TableSpec parent) {
    this.parent = parent;
    this.depth = super.depth();
  }

  /**
   * The depth of the table is the maximum depth of its dependencies plus one. A table with no dependencies has a
   * depth of zero.
   * @return the depth
   */
  @Override
  public int depth() {
    return depth;
  }

  /**
   * @return The value of the {@code parent} attribute
   */
  @Override
  public TableSpec parent() {
    return parent;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ReverseTable#parent() parent} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for parent
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableReverseTable withParent(TableSpec value) {
    if (this.parent == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "parent");
    return new ImmutableReverseTable(this, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableReverseTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableReverseTable
        && equalTo(0, (ImmutableReverseTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableReverseTable another) {
    return depth == another.depth
        && parent.equals(another.parent);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code parent}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + parent.hashCode();
    return h;
  }

  /**
   * Construct a new immutable {@code ReverseTable} instance.
   * @param parent The value for the {@code parent} attribute
   * @return An immutable ReverseTable instance
   */
  public static ImmutableReverseTable of(TableSpec parent) {
    return new ImmutableReverseTable(parent);
  }

  /**
   * Creates an immutable copy of a {@link ReverseTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ReverseTable instance
   */
  public static ImmutableReverseTable copyOf(ReverseTable instance) {
    if (instance instanceof ImmutableReverseTable) {
      return (ImmutableReverseTable) instance;
    }
    return ImmutableReverseTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableReverseTable ImmutableReverseTable}.
   * <pre>
   * ImmutableReverseTable.builder()
   *    .parent(io.deephaven.qst.table.TableSpec) // required {@link ReverseTable#parent() parent}
   *    .build();
   * </pre>
   * @return A new ImmutableReverseTable builder
   */
  public static ImmutableReverseTable.Builder builder() {
    return new ImmutableReverseTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableReverseTable ImmutableReverseTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ReverseTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_PARENT = 0x1L;
    private long initBits = 0x1L;

    private TableSpec parent;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.ReverseTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(ReverseTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code io.deephaven.qst.table.SingleParentTable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(SingleParentTable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      long bits = 0;
      if (object instanceof ReverseTable) {
        ReverseTable instance = (ReverseTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
      if (object instanceof SingleParentTable) {
        SingleParentTable instance = (SingleParentTable) object;
        if ((bits & 0x1L) == 0) {
          parent(instance.parent());
          bits |= 0x1L;
        }
      }
    }

    /**
     * Initializes the value for the {@link ReverseTable#parent() parent} attribute.
     * @param parent The value for parent 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder parent(TableSpec parent) {
      this.parent = Objects.requireNonNull(parent, "parent");
      initBits &= ~INIT_BIT_PARENT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableReverseTable ImmutableReverseTable}.
     * @return An immutable instance of ReverseTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableReverseTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableReverseTable(null, parent);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_PARENT) != 0) attributes.add("parent");
      return "Cannot build ReverseTable, some of required attributes are not set " + attributes;
    }
  }
}
