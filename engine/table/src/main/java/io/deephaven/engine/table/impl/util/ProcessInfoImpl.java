//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.process.ProcessInfo;
import io.deephaven.process.ProcessInfoLogVisitor;
import io.deephaven.process.ProcessUniqueId;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders4;

import java.io.IOException;
import java.util.Objects;

class ProcessInfoImpl {

    private final ProcessUniqueId id;
    private ProcessInfoLogLogger delegate;
    private Table table;

    public ProcessInfoImpl(ProcessUniqueId id, ProcessInfoLogLogger delegate) {
        this.id = Objects.requireNonNull(id);
        this.delegate = Objects.requireNonNull(delegate);
    }

    public void init(ProcessInfo pInfo) throws IOException {
        final Visitor visitor = new Visitor();
        pInfo.traverse(visitor);
        table = visitor.table();
        delegate.close();
        delegate = null;
    }

    public Table table() {
        return Objects.requireNonNull(table);
    }

    private class Visitor extends ProcessInfoLogVisitor {
        private final ColumnHeaders4<String, String, String, String>.Rows rows;

        public Visitor() {
            final ColumnHeader<String> id = ColumnHeader.ofString("Id");
            final ColumnHeader<String> type = ColumnHeader.ofString("Type");
            final ColumnHeader<String> key = ColumnHeader.ofString("Key");
            final ColumnHeader<String> value = ColumnHeader.ofString("Value");
            final ColumnHeaders4<String, String, String, String> header = id.header(type).header(key).header(value);
            rows = header.start(1024);
        }

        @Override
        protected void log(final String type, final String key, final String value) throws IOException {
            rows.row(id.value(), type, key, value);
            delegate.log(id.value(), type, key, value);
        }

        Table table() {
            return TableFactory.newTable(rows);
        }
    }
}
