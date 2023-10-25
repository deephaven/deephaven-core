/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.util.List;

public interface TableService extends TableHandleManager {

    // ----------------------------------------------------------

    /**
     * Creates a new export with a {@link ExportRequest.Listener#logging() logging listener}.
     *
     * <p>
     * Equivalent to {@code export(ExportsRequest.logging(table)).get(0)}.
     *
     * @param table the table
     * @return the export
     */
    @Deprecated
    Export export(TableSpec table);

    /**
     * Creates new exports according to the {@code request}.
     *
     * @param request the request
     * @return the exports
     */
    @Deprecated
    List<Export> export(ExportsRequest request);

    // ----------------------------------------------------------

    /**
     * A batch table handle manager.
     *
     * @return a batch manager
     */
    TableHandleManager batch();

    /**
     * A batch table handle manager.
     *
     * @param mixinStacktraces if stacktraces should be mixin
     * @return a batch manager
     */
    TableHandleManager batch(boolean mixinStacktraces);

    /**
     * A serial table handle manager.
     *
     * @return a serial manager
     */
    TableHandleManager serial();

    // ----------------------------------------------------------
}
