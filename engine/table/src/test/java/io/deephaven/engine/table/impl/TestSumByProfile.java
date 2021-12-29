package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetTools;

import java.text.DecimalFormat;

public class TestSumByProfile {
    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
        }
        final String filename = args[1];

        final boolean view;
        switch (args[0]) {
            case "view":
                view = true;
                break;
            case "select":
                view = false;
                break;
            default:
                view = true;
                usage();
        }
        System.out.println("Reading: " + filename);
        final Table relation = ParquetTools.readTable(filename);
        final long startTimeSelect = System.nanoTime();
        final Table viewed = view ? relation.view("animal_id", "Values") : relation.select("animal_id", "Values");
        final long endTimeSelect = System.nanoTime();
        System.out.println("Select Elapsed Time: " + new DecimalFormat("###,###.000")
                .format(((double) (endTimeSelect - startTimeSelect) / 1_000_000_000.0)));
        final Table sumBy = viewed.sumBy("animal_id");
        final long endTimeSum = System.nanoTime();
        System.out.println("Sum Elapsed Time: "
                + new DecimalFormat("###,###.000").format(((double) (endTimeSum - endTimeSelect) / 1_000_000_000.0)));
        TableTools.show(sumBy);
    }

    private static void usage() {
        System.err.println("TestSumByProfile select|view filename");
        System.exit(1);
    }
}
