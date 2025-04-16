//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.TypedArray;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.base.Js;

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

    // Some ticket types use a slash as a delimeter between fields
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
        Ticket ticket = new Ticket();
        ticket.setTicket(varDef.getId());
        return ticket;
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
        TableReference tableRef = new TableReference();
        tableRef.setTicket(createTicket(varDef));
        return tableRef;
    }

    public static void validateScopeOrApplicationTicketBase64(String base64Bytes) {
        String bytes = DomGlobal.atob(base64Bytes);
        if (bytes.length() > 2) {
            String prefix = bytes.substring(0, 2);
            if ((prefix.charAt(0) == SCOPE_PREFIX || prefix.charAt(0) == APPLICATION_PREFIX)
                    && prefix.charAt(1) == TICKET_DELIMITER) {
                return;
            }
        }
        throw new IllegalArgumentException("Cannot create a VariableDefinition from a non-scope ticket");
    }

    /**
     * Provides the next export id for the current session as a ticket.
     *
     * @return a new ticket with an export id that hasn't previously been used for this session
     */
    public Ticket newExportTicket() {
        Ticket ticket = new Ticket();
        ticket.setTicket(newExportTicketRaw());
        return ticket;
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

    private Uint8Array newExportTicketRaw() {
        final int exportId = newTicketInt();
        final double[] dest = new double[5];
        dest[0] = EXPORT_PREFIX;
        dest[1] = (byte) exportId;
        dest[2] = (byte) (exportId >>> 8);
        dest[3] = (byte) (exportId >>> 16);
        dest[4] = (byte) (exportId >>> 24);

        final Uint8Array bytes = new Uint8Array(5);
        bytes.set(dest);
        return bytes;
    }

    /**
     * Provides the next export id for the current session as a table ticket.
     *
     * @return a new table ticket with an export id that hasn't previously been used for this session
     */
    public TableTicket newTableTicket() {
        return new TableTicket(newExportTicketRaw());
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
        int length = Js.asArrayLike(array).getLength();
        Uint8Array bytesWithPrefix = new Uint8Array(length + 2);
        // Add the shared ticket prefix at the start of the provided value
        bytesWithPrefix.setAt(0, (double) SHARED_PREFIX);
        bytesWithPrefix.setAt(1, (double) TICKET_DELIMITER);
        bytesWithPrefix.set(array, 2);

        Ticket ticket = new Ticket();
        ticket.setTicket(bytesWithPrefix);
        return ticket;
    }
}
