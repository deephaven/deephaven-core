package io.deephaven.qst.table;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TicketTable}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTicketTable.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code ImmutableTicketTable.of()}.
 */
@Generated(from = "TicketTable", generator = "Immutables")
@SuppressWarnings({"all"})
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableTicketTable extends TicketTable {
  private final int depth;
  private final byte[] ticket;

  private ImmutableTicketTable(byte[] ticket) {
    this.ticket = ticket.clone();
    this.depth = super.depth();
  }

  private ImmutableTicketTable(ImmutableTicketTable original, byte[] ticket) {
    this.ticket = ticket;
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
   * The ticket.
   * @return the ticket
   */
  @Override
  public byte[] ticket() {
    return ticket.clone();
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link TicketTable#ticket() ticket}.
   * The array is cloned before being saved as attribute values.
   * @param elements The non-null elements for ticket
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTicketTable withTicket(byte... elements) {
    byte[] newValue = elements.clone();
    return validate(new ImmutableTicketTable(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTicketTable} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTicketTable
        && equalTo(0, (ImmutableTicketTable) another);
  }

  private boolean equalTo(int synthetic, ImmutableTicketTable another) {
    return depth == another.depth
        && Arrays.equals(ticket, another.ticket);
  }

  /**
   * Computes a hash code from attributes: {@code depth}, {@code ticket}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int h = 5381;
    h += (h << 5) + depth;
    h += (h << 5) + Arrays.hashCode(ticket);
    return h;
  }

  /**
   * Construct a new immutable {@code TicketTable} instance.
   * @param ticket The value for the {@code ticket} attribute
   * @return An immutable TicketTable instance
   */
  public static ImmutableTicketTable of(byte[] ticket) {
    return validate(new ImmutableTicketTable(ticket));
  }

  private static ImmutableTicketTable validate(ImmutableTicketTable instance) {
    instance.checkNonEmpty();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link TicketTable} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TicketTable instance
   */
  public static ImmutableTicketTable copyOf(TicketTable instance) {
    if (instance instanceof ImmutableTicketTable) {
      return (ImmutableTicketTable) instance;
    }
    return ImmutableTicketTable.builder()
        .from(instance)
        .build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableTicketTable ImmutableTicketTable}.
   * <pre>
   * ImmutableTicketTable.builder()
   *    .ticket(byte) // required {@link TicketTable#ticket() ticket}
   *    .build();
   * </pre>
   * @return A new ImmutableTicketTable builder
   */
  public static ImmutableTicketTable.Builder builder() {
    return new ImmutableTicketTable.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTicketTable ImmutableTicketTable}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TicketTable", generator = "Immutables")
  public static final class Builder {
    private static final long INIT_BIT_TICKET = 0x1L;
    private long initBits = 0x1L;

    private byte[] ticket;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TicketTable} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder from(TicketTable instance) {
      Objects.requireNonNull(instance, "instance");
      ticket(instance.ticket());
      return this;
    }

    /**
     * Initializes the value for the {@link TicketTable#ticket() ticket} attribute.
     * @param ticket The elements for ticket
     * @return {@code this} builder for use in a chained invocation
     */
    public final Builder ticket(byte... ticket) {
      this.ticket = ticket.clone();
      initBits &= ~INIT_BIT_TICKET;
      return this;
    }

    /**
     * Builds a new {@link ImmutableTicketTable ImmutableTicketTable}.
     * @return An immutable instance of TicketTable
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTicketTable build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableTicketTable.validate(new ImmutableTicketTable(null, ticket));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TICKET) != 0) attributes.add("ticket");
      return "Cannot build TicketTable, some of required attributes are not set " + attributes;
    }
  }
}
