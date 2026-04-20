//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import elemental2.core.TypedArray;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;

/**
 * Single factory for known ticket types. By definition, this cannot be exhaustive, since flight tickets have no
 * inherent structure - Deephaven Core only specifies that the first byte will indicate the type of ticket, and later
 * bytes will be handled by handlers for that type. Deephaven Core requires only that export tickets be support, but
 * also offers application tickets, scope tickets, and shared tickets. Downstream projects may define new ticket types,
 * which won't necessarily be understood by this client.
 *
 * @see io.deephaven.server.session.ExportTicketResolver
 * @see io.deephaven.server.appmode.ApplicationTicketResolver
 * @see io.deephaven.server.console.ScopeTicketResolver
 * @see io.deephaven.server.session.SharedTicketResolver
 */
public class Tickets {
    // Prefix for all export tickets
    private static final byte EXPORT_PREFIX = 'e';
    // Prefix for all application tickets
    private static final byte APPLICATION_PREFIX = 'a';
    // Prefix for all scope tickets
    private static final byte SCOPE_PREFIX = 's';
    // Prefix for all shared tickets
    private static final byte SHARED_PREFIX = 'h';

    // Some ticket types use a slash as a delimiter between fields
    private static final char TICKET_DELIMITER = '/';

    /**
     * The next number to use when making an export ticket. These values must always be positive, as zero is an invalid
     * value, and negative values represent server-created tickets.
     */
    private int nextExport = 1;

    public Tickets() {}

    /**
     * Utility method to create a ticket from a known-valid base64 encoding of a ticket.
     * <p>
     * Use caution with non-export tickets, the definition may change between calls to the server - they should be
     * exported before use.
     *
     * @param varDef the variable definition to create a ticket from
     * @return a ticket with the variable's id as the ticket bytes
     */
    public static Ticket createTicket(JsVariableDefinition varDef) {
        return Ticket.newBuilder().setTicket(ByteString.copyFrom(BaseEncoding.base64().decode(varDef.getId()))).build();
    }

    /**
     * Utility method to create a ticket wrapped in a TableReference from a known-valid base64 encoding of a ticket.
     * <p>
     * Use caution with non-export tickets, the definition may change between calls to the server - they should be
     * exported before use.
     *
     * @param varDef the variable definition to create a ticket from
     * @return a table reference with the variable's id as the ticket bytes
     */

    public static TableReference createTableRef(JsVariableDefinition varDef) {
        return TableReference.newBuilder()
                .setTicket(createTicket(varDef))
                .build();
    }

    public static void validateScopeOrApplicationTicketBase64(String base64Bytes) {
        byte[] bytes = BaseEncoding.base64().decode(base64Bytes);

        if (bytes.length > 2) {
            if ((bytes[0] == SCOPE_PREFIX || bytes[0] == APPLICATION_PREFIX) && bytes[1] == TICKET_DELIMITER) {
                return;
            }
        }
        throw new IllegalArgumentException("Can only create a VariableDefinition from scope or application tickets");
    }

    /**
     * Provides the next export id for the current session as a ticket.
     *
     * @return a new ticket with an export id that hasn't previously been used for this session
     */
    public Ticket newExportTicket() {
        final int exportId = newTicketInt();
        final byte[] bytes = new byte[5];
        bytes[0] = EXPORT_PREFIX;
        bytes[1] = (byte) exportId;
        bytes[2] = (byte) (exportId >>> 8);
        bytes[3] = (byte) (exportId >>> 16);
        bytes[4] = (byte) (exportId >>> 24);

        return Ticket.newBuilder().setTicket(ByteString.copyFrom(bytes)).build();
    }

    /**
     * Provides the next export id for the current session.
     *
     * @return the next export id
     */
    public int newTicketInt() {
        if (nextExport == Integer.MAX_VALUE) {
            throw new IllegalStateException("Ran out of tickets!");
        }

        return nextExport++;
    }

    /**
     * Provides the next export id for the current session as a table ticket.
     *
     * @return a new table ticket with an export id that hasn't previously been used for this session
     */
    public TableTicket newTableTicket() {
        return new TableTicket(newExportTicket());
    }

    /**
     * Creates a shared ticket from the provided array of bytes.
     * <p>
     * Use caution with non-export tickets, the definition may change between calls to the server - they should be
     * exported before use.
     *
     * @param array array of bytes to populate the ticket with
     * @return a new shared ticket
     */
    public Ticket sharedTicket(TypedArray.SetArrayUnionType array) {
        JsArrayLike<Object> arrayLike = Js.asArrayLike(array);
        int length = arrayLike.getLength();
        byte[] bytes = new byte[length + 2];
        // Add the shared ticket prefix at the start of the provided value
        bytes[0] = SHARED_PREFIX;
        bytes[1] = TICKET_DELIMITER;
        for (int i = 0; i < length; i++) {
            bytes[i + 2] = (byte) (double) arrayLike.getAt(i);
        }

        return Ticket.newBuilder().setTicket(ByteString.copyFrom(bytes)).build();
    }
}
