package io.deephaven.client.impl;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
<<<<<<< HEAD
import io.deephaven.qst.table.TableSpec;
=======
>>>>>>> main
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ExportRequest}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableExportRequest.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableExportRequest.of()}.
 */
@Generated(from = "ExportRequest", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableExportRequest extends ExportRequest {
  private final TableSpec table;
  private final ExportRequest.Listener listener;

  private ImmutableExportRequest(TableSpec table, ExportRequest.Listener listener) {
    this.table = Objects.requireNonNull(table, "table");
    this.listener = Objects.requireNonNull(listener, "listener");
  }

  private ImmutableExportRequest(
      ImmutableExportRequest original,
      TableSpec table,
      ExportRequest.Listener listener) {
    this.table = table;
    this.listener = listener;
  }

  /**
   * The table.
   * @return the table
   */
  @Override
  public TableSpec table() {
    return table;
  }

  /**
   * The listener.
   * @return the listener
   */
  @Override
  public ExportRequest.Listener listener() {
    return listener;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ExportRequest#table() table} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for table
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableExportRequest withTable(TableSpec value) {
    if (this.table == value) return this;
    TableSpec newValue = Objects.requireNonNull(value, "table");
    return new ImmutableExportRequest(this, newValue, this.listener);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ExportRequest#listener() listener} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for listener
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableExportRequest withListener(ExportRequest.Listener value) {
    if (this.listener == value) return this;
    ExportRequest.Listener newValue = Objects.requireNonNull(value, "listener");
    return new ImmutableExportRequest(this, this.table, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableExportRequest} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableExportRequest
        && equalTo(0, (ImmutableExportRequest) another);
  }

  private boolean equalTo(int synthetic, ImmutableExportRequest another) {
    return table.equals(another.table)
        && listener.equals(another.listener);
  }

  /**
   * Computes a hash code from attributes: {@code table}, {@code listener}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + table.hashCode();
    h += (h << 5) + listener.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ExportRequest} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ExportRequest")
        .omitNullValues()
        .add("table", table)
        .add("listener", listener)
        .toString();
  }

  /**
   * Construct a new immutable {@code ExportRequest} instance.
   * @param table The value for the {@code table} attribute
   * @param listener The value for the {@code listener} attribute
   * @return An immutable ExportRequest instance
   */
  public static ImmutableExportRequest of(TableSpec table, ExportRequest.Listener listener) {
    return new ImmutableExportRequest(table, listener);
  }

  /**
   * Creates an immutable copy of a {@link ExportRequest} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ExportRequest instance
   */
  public static ImmutableExportRequest copyOf(ExportRequest instance) {
    if (instance instanceof ImmutableExportRequest) {
      return (ImmutableExportRequest) instance;
    }
    return ImmutableExportRequest.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableExportRequest ImmutableExportRequest}.
   * <pre>
   * ImmutableExportRequest.builder()
<<<<<<< HEAD
   *    .table(io.deephaven.qst.table.TableSpec) // required {@link ExportRequest#table() table}
=======
   *    .table(io.deephaven.client.impl.TableSpec) // required {@link ExportRequest#table() table}
>>>>>>> main
   *    .listener(io.deephaven.client.impl.ExportRequest.Listener) // required {@link ExportRequest#listener() listener}
   *    .build();
   * </pre>
   * @return A new ImmutableExportRequest builder
   */
  public static ImmutableExportRequest.Builder builder() {
    return new ImmutableExportRequest.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableExportRequest ImmutableExportRequest}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ExportRequest", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TABLE = 0x1L;
    private static final long INIT_BIT_LISTENER = 0x2L;
    private long initBits = 0x3L;

    private @Nullable TableSpec table;
    private @Nullable ExportRequest.Listener listener;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ExportRequest} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ExportRequest instance) {
      Objects.requireNonNull(instance, "instance");
      table(instance.table());
      listener(instance.listener());
      return this;
    }

    /**
     * Initializes the value for the {@link ExportRequest#table() table} attribute.
     * @param table The value for table 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder table(TableSpec table) {
      this.table = Objects.requireNonNull(table, "table");
      initBits &= ~INIT_BIT_TABLE;
      return this;
    }

    /**
     * Initializes the value for the {@link ExportRequest#listener() listener} attribute.
     * @param listener The value for listener 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder listener(ExportRequest.Listener listener) {
      this.listener = Objects.requireNonNull(listener, "listener");
      initBits &= ~INIT_BIT_LISTENER;
      return this;
    }

    /**
     * Builds a new {@link ImmutableExportRequest ImmutableExportRequest}.
     * @return An immutable instance of ExportRequest
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableExportRequest build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableExportRequest(null, table, listener);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TABLE) != 0) attributes.add("table");
      if ((initBits & INIT_BIT_LISTENER) != 0) attributes.add("listener");
      return "Cannot build ExportRequest, some of required attributes are not set " + attributes;
    }
  }
}
