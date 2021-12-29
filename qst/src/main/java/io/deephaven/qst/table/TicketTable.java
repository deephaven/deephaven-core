package io.deephaven.qst.table;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.charset.StandardCharsets;

/**
 * A ticket table is a byte format that allows callers to reference an existing table via ticket. The ticket bytes are
 * opaque; the byte-format may change from release to release.
 */
@Immutable
@SimpleStyle
public abstract class TicketTable extends TableBase {

    /**
     * Create a ticket table with the {@code ticket} bytes.
     *
     * @param ticket the ticket
     * @return the ticket table
     */
    public static TicketTable of(byte[] ticket) {
        return ImmutableTicketTable.of(ticket);
    }

    /**
     * Create a ticket table with the UTF-8 bytes from the {@code ticket} string.
     *
     * @param ticket the ticket string
     * @return the ticket table
     */
    public static TicketTable of(String ticket) {
        return ImmutableTicketTable.of(ticket.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Create a ticket table from the provided {@code fieldName}.
     *
     * @param fieldName the query scope field name
     * @return the ticket table
     */
    public static TicketTable fromQueryScopeField(String fieldName) {
        return of("s/" + fieldName);
    }

    /**
     * Create a ticket table from the provided {@code applicationId} and {@code fieldName}.
     *
     * @param applicationId the application id
     * @param fieldName the application field name
     * @return the ticket table
     */
    public static TicketTable fromApplicationField(String applicationId, String fieldName) {
        return of("a/" + applicationId + "/f/" + fieldName);
    }

    /**
     * The ticket.
     *
     * @return the ticket
     */
    @Parameter
    public abstract byte[] ticket();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNonEmpty() {
        if (ticket().length == 0) {
            throw new IllegalArgumentException("Ticket must be non-empty");
        }
    }
}
