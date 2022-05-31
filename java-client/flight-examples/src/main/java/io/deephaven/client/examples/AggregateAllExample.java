package io.deephaven.client.examples;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.LabeledTables.Builder;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Arrays;
import java.util.List;

@Command(name = "aggregate-all", mixinStandardHelpOptions = true,
        description = "Aggregate all examples", version = "0.1.0")
class AggregateAllExample extends AggInputBase {

    private static List<AggSpec> aggSpecs() {
        return Arrays.asList(
                AggSpec.absSum(),
                AggSpec.avg(),
                // Key.countDistinct(false),
                // Key.countDistinct(true),
                // Key.distinct(false),
                // Key.distinct(true),
                AggSpec.first(),
                AggSpec.group(),
                AggSpec.last(),
                AggSpec.max(),
                // Key.median(false),
                AggSpec.median(true),
                AggSpec.min(),
                // Key.percentile(0.25, false),
                // Key.percentile(0.25, true),
                // Key.sortedFirst(...),
                // Key.sortedLast(...),
                AggSpec.std(),
                AggSpec.sum(),
                // Key.unique(false),
                // Key.unique(true),
                AggSpec.var(),
                AggSpec.wavg("Z")
        // Key.wsum("Z")
        );
    }

    @Override
    public LabeledTables labeledTables(TableSpec base) {
        final Builder builder = LabeledTables.builder();
        for (AggSpec aggSpec : aggSpecs()) {
            final String name = aggSpec.toString()
                    .replace('=', '_')
                    .replace('{', '_')
                    .replace('}', '_')
                    .replace(' ', '_')
                    .replace('.', '_');
            final TableSpec tableSpec = base.aggAllBy(aggSpec, GROUP_KEY.name()).sort(GROUP_KEY.name());
            builder.putMap(name, tableSpec);
        }
        return builder.build();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AggregateAllExample()).execute(args);
        System.exit(execute);
    }
}
