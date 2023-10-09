package io.deephaven.qst.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link LabeledTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableLabeledTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableLabeledTable.of()}.
 */
@Generated(from = "LabeledTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableLabeledTable extends LabeledTable {
  private final String label;
  private final TableSpec table;

  private ImmutableLabeledTable(String label, TableSpec table) {
    this.label = Objects.requireNonNull(label, "label");
    this.table = Objects.requireNonNull(table, "table");
  }

  private ImmutableLabeledTable(ImmutableLabeledTable original, String label, TableSpec table) {
    this.label = label;
    this.table = table;
  }

  /**
   * @return The value of the {@code label} attribute
   */
  @Override
  public String label() {
    return label;
  }

  /**
   * @return The value of the {@code table} attribute
   */
  @Override
  public TableSpec table() {
    return table;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LabeledTable#label() label} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for label
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLabeledTable withLabel(String value) {
    String newValue = Objects.requireNonNull(value, "label");
    if (this.label.equals(newValue)) return this;
    return validate(new ImmutableLabeledTable(this, newValue, this.table));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link LabeledTable#table() table} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for table
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableLabeledTable withTable(TableSpec value) {
    if (this.table == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "table");
    return validate(new ImmutableLabeledTable(this, this.label, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableLabeledTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableLabeledTable
        && equalTo(0, (ImmutableLabeledTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableLabeledTable another) {
    return label.equals(another.label)
        && table.equals(another.table);
  }

  /**
   * Computes a hash code from attributes: {@code label}, {@code table}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + label.hashCode();
    h += (h << 5) + table.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code LabeledTable} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "LabeledTable{"
        + "label=" + label
        + ", table=" + table
        + "}";
  }

  /**
   * Construct a new immutable {@code LabeledTable} instance.
   * @param label The value for the {@code label} attribute
   * @param table The value for the {@code table} attribute
   * @return An immutable LabeledTable instance
   */
  public static ImmutableLabeledTable of(String label, TableSpec table) {
    return validate(new ImmutableLabeledTable(label, table));
  }

  private static ImmutableLabeledTable validate(ImmutableLabeledTable instance) {
    instance.checkNotEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link LabeledTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable LabeledTable instance
   */
  public static ImmutableLabeledTable copyOf(LabeledTable instance) {
    if (instance instanceof ImmutableLabeledTable) {
      return (ImmutableLabeledTable) instance;
    }
    return ImmutableLabeledTable.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableLabeledTable ImmutableLabeledTable}.
   * <pre>
   * ImmutableLabeledTable.builder()
   *    .label(String) // required {@link LabeledTable#label() label}
   *    .table(io.deephaven.qst.table.TableSpec) // required {@link LabeledTable#table() table}
   *    .build();
   * </pre>
   * @return A new ImmutableLabeledTable builder
   */
  public static ImmutableLabeledTable.Builder builder() {
    return new ImmutableLabeledTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableLabeledTable ImmutableLabeledTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "LabeledTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_LABEL = 0x1L;
    private static final long INIT_BIT_TABLE = 0x2L;
    private long initBits = 0x3L;

    private String label;
    private TableSpec table;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code LabeledTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(LabeledTable instance) {
      Objects.requireNonNull(instance, "instance");
      label(instance.label());
      table(instance.table());
      return this;
    }

    /**
     * Initializes the value for the {@link LabeledTable#label() label} attribute.
     * @param label The value for label 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder label(String label) {
      this.label = Objects.requireNonNull(label, "label");
      initBits &= ~INIT_BIT_LABEL;
      return this;
    }

    /**
     * Initializes the value for the {@link LabeledTable#table() table} attribute.
     * @param table The value for table 
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder table(TableSpec table) {
      this.table = Objects.requireNonNull(table, "table");
      initBits &= ~INIT_BIT_TABLE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableLabeledTable ImmutableLabeledTable}.
     * @return An immutable instance of LabeledTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableLabeledTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableLabeledTable.validate(new ImmutableLabeledTable(null, label, table));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_LABEL) != 0) attributes.add("label");
      if ((initBits & INIT_BIT_TABLE) != 0) attributes.add("table");
      return "Cannot build LabeledTable, some of required attributes are not set " + attributes;
    }
  }
}
