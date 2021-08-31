package io.deephaven.qst.table;

import io.deephaven.annotations.SimpleStyle;
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

    public static TicketTable of(byte[] ticket) {
        return ImmutableTicketTable.of(ticket);
    }

    public static TicketTable of(String ticket) {
        return ImmutableTicketTable.of(ticket.getBytes(StandardCharsets.UTF_8));
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
}
