/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.Table;

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
        TableTools.writeCsv(source, args[1], false, DBTimeZone.TZ_DEFAULT, false, columns);
    }
}
