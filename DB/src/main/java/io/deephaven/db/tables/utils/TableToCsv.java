/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import java.io.IOException;

public class TableToCsv {

    public static void main(String[] args) throws IOException {
        String columns[];
        if (args.length > 2) {
            columns = args[2].split(",");
        } else {
            columns = new String[0];
        }

        TableTools.writeCsv(args[0], args[1], columns);
    }
}
