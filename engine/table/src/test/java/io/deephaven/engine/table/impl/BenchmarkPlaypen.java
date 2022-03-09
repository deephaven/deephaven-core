package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.AutoTuningIncrementalReleaseFilter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.parquet.table.ParquetTools;

import java.text.DecimalFormat;

/**
 * This program will read a parquet file, select or view some columns, and then perform some operation on the resulting
 * table. It mimics some of the tests we would care about with bencher (https://github.com/deephaven/bencher), but in an
 * easy-to-profile way from your IDE.
 */
public class BenchmarkPlaypen {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4) {
            usage();
        }
        final String filename = args[3];

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

        final boolean incremental;
        switch (args[1]) {
            case "static":
                incremental = false;
                break;
            case "incremental":
                incremental = true;
                break;
            default:
                incremental = false;
                usage();
        }

        final String mode;
        switch (args[2]) {
            case "sum":
            case "count":
            case "count2":
            case "countplant":
            case "countmany":
            case "noop":
            case "sumall":
                mode = args[2];
                break;
            default:
                mode = "";
                usage();
        }
        System.out.println("Reading: " + filename);
        final long readStartTime = System.nanoTime();
        final Table relation = ParquetTools.readTable(filename);
        final long startTimeSelect = System.nanoTime();
        System.out.println("readTable Elapsed Time: " + new DecimalFormat("###,###.000")
                .format(((double) (startTimeSelect - readStartTime) / 1_000_000_000.0)));
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
            case "countmany":
                columns = new String[] {"key1"};
                break;
            case "noop":
            case "sumall":
                columns = new String[] {"animal_id", "adjective_id", "plant_id", "Values"};
                break;
            default:
                throw new IllegalArgumentException("Invalid mode " + mode);
        }
        final Table viewed = view ? relation.view(columns) : relation.select(columns);
        final long endTimeSelect = System.nanoTime();
        System.out.println("Select Elapsed Time: " + new DecimalFormat("###,###.000")
                .format(((double) (endTimeSelect - startTimeSelect) / 1_000_000_000.0)));

        final Table input;
        final AutoTuningIncrementalReleaseFilter filter;
        if (incremental) {
            System.out.println("Running test incrementally.");
            UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
            filter = new AutoTuningIncrementalReleaseFilter(new StreamLoggerImpl(), 0, 1_000_000L, 1.0, true);
            input = viewed.where(filter);
        } else {
            System.out.println("Running test statically.");
            input = viewed;
            filter = null;
        }

        final Table result;
        switch (mode) {
            case "sum":
                result = input.sumBy("animal_id");
                break;
            case "sumall":
                result = input.sumBy();
                break;
            case "count":
                result = input.countBy("N", "animal_id");
                break;
            case "count2":
                result = input.countBy("N", "animal_id", "adjective_id");
                break;
            case "countplant":
                result = input.countBy("N", "plant_id");
                break;
            case "countmany":
                result = input.countBy("N", "key1");
                break;
            case "noop":
                result = TableTools.emptyTable(0);
                break;
            default:
                throw new IllegalArgumentException("Invalid mode " + mode);
        }

        if (filter != null) {
            filter.start();
            while (viewed.size() > input.size()) {
                final long initialSize = input.size();
                System.out.println("Running UpdateGraphProcessor cycle: " + input.size() + " / " + viewed.size());
                UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(filter::run);
                if (initialSize == input.size()) {
                    throw new RuntimeException("Did not increase size of input table during cycle!");
                }
            }
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
