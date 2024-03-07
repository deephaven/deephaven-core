//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.engine.tablelogger.ProcessInfoLogLogger;
import io.deephaven.process.ProcessInfo;
import io.deephaven.process.ProcessUniqueId;
import io.deephaven.properties.PropertyVisitorStringBase;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.column.header.ColumnHeaders4;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

class ProcessInfoImpl {

    private final ProcessUniqueId id;
    private final ProcessInfoLogLogger delegate;
    private Table table;

    public ProcessInfoImpl(ProcessUniqueId id, ProcessInfoLogLogger delegate) {
        this.id = Objects.requireNonNull(id);
        this.delegate = Objects.requireNonNull(delegate);
    }

    public void init(ProcessInfo pInfo) throws IOException {
        final Visitor visitor = new Visitor();
        pInfo.traverse(visitor);
        table = visitor.table();
    }

    class Visitor extends PropertyVisitorStringBase {
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
        public void visit(final String key, String value) {
            final int ix1 = key.indexOf('.');
            final String type1 = key.substring(0, ix1);
            final String remaining = key.substring(ix1 + 1);
            final int ix2 = remaining.indexOf('.');
            if (ix2 == -1) {
                try {
                    log(type1, remaining, value);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return;
            }
            final String type2 = remaining.substring(0, ix2);
            final String remaining2 = remaining.substring(ix2 + 1);
            try {
                log(type1 + "." + type2, remaining2, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void log(final String type, final String key, final String value) throws IOException {
            rows.row(id.value(), type, key, value);
            delegate.log(id.value(), type, key, value);
        }

        public Table table() {
            return TableFactory.newTable(rows);
        }
    }

    public Table table() {
        return Objects.requireNonNull(table);
    }
}
