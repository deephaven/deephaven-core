package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetTools;

import java.text.DecimalFormat;

/**
 * This program will read a parquet file, select or view some columns, and then perform some operation on the resulting
 * table. It mimics some of the tests we would care about with bencher (https://github.com/deephaven/bencher), but in an
 * easy-to-profile way from your IDE.
 */
public class BenchmarkPlaypen {
    public static void main(String[] args) {
        if (args.length != 3) {
            usage();
        }
        final String filename = args[2];

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
        final String mode;
        switch (args[1]) {
            case "sum":
            case "count":
            case "count2":
            case "countplant":
            case "noop":
                mode = args[1];
                break;
            default:
                mode = "";
                usage();
        }
        System.out.println("Reading: " + filename);
        final Table relation = ParquetTools.readTable(filename);
        final long startTimeSelect = System.nanoTime();
        final String[] columns;
        switch (mode) {
            case "sum":
                columns = new String[] {"animal_id", "Values"};
                break;
            case "count":
                columns = new String[] {"animal_id"};
                break;
            case "count2":
                columns = new String[] {"animal_id", "adjective_id"};
                break;
            case "countplant":
                columns = new String[] {"plant_id"};
                break;
            case "noop":
                columns = new String[] {"animal_id", "adjective_id", "plant_id"};
                break;
            default:
                throw new IllegalArgumentException("Invalid mode " + mode);
        }
        final Table viewed = view ? relation.view(columns) : relation.select(columns);
        final long endTimeSelect = System.nanoTime();
        System.out.println("Select Elapsed Time: " + new DecimalFormat("###,###.000")
                .format(((double) (endTimeSelect - startTimeSelect) / 1_000_000_000.0)));
        final Table result;
        switch (mode) {
            case "sum":
                result = viewed.sumBy("animal_id");
                break;
            case "count":
                result = viewed.countBy("N", "animal_id");
                break;
            case "count2":
                result = viewed.countBy("N", "animal_id", "adjective_id");
                break;
            case "countplant":
                result = viewed.countBy("N", "plant_id");
                break;
            case "noop":
                result = TableTools.emptyTable(0);
                break;
            default:
                throw new IllegalArgumentException("Invalid mode " + mode);
        }
        final long endTimeSum = System.nanoTime();
        System.out.println("Operation Elapsed Time: "
                + new DecimalFormat("###,###.000").format(((double) (endTimeSum - endTimeSelect) / 1_000_000_000.0)));
        TableTools.show(result);
    }

    private static void usage() {
        System.err.println("TestSumByProfile select|view sum|count filename");
        System.exit(1);
    }
}
