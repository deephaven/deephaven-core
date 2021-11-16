/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.time.TimeZone;

import java.io.IOException;

public class TableToCsv {

    public static void main(String[] args) throws IOException {
        String columns[];
        if (args.length > 2) {
            columns = args[2].split(",");
        } else {
            columns = new String[0];
        }

        Table source = ParquetTools.readTable(args[0]);
        TableTools.writeCsv(source, args[1], false, TimeZone.TZ_DEFAULT, false, columns);
    }
}
