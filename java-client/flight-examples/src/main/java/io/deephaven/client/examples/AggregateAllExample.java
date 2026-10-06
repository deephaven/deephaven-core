//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.LabeledTables.Builder;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.LinkedHashMap;
import java.util.Map;

@Command(name = "aggregate-all", mixinStandardHelpOptions = true,
        description = "Aggregate all examples", version = "0.1.0")
class AggregateAllExample extends AggInputBase {

    /**
     * The aggregations to publish, keyed by the variable name each result is published under.
     */
    private static Map<String, AggSpec> aggSpecs() {
        final Map<String, AggSpec> specs = new LinkedHashMap<>();
        specs.put("absSum", AggSpec.absSum());
        specs.put("avg", AggSpec.avg());
        specs.put("first", AggSpec.first());
        specs.put("group", AggSpec.group());
        specs.put("last", AggSpec.last());
        specs.put("max", AggSpec.max());
        specs.put("median", AggSpec.median(true));
        specs.put("min", AggSpec.min());
        specs.put("std", AggSpec.std());
        specs.put("sum", AggSpec.sum());
        specs.put("var", AggSpec.var());
        specs.put("wavg", AggSpec.wavg("Z"));
        return specs;
    }

    @Override
    public LabeledTables labeledTables(TableSpec base) {
        final Builder builder = LabeledTables.builder();
        for (Map.Entry<String, AggSpec> e : aggSpecs().entrySet()) {
            final TableSpec tableSpec = base.aggAllBy(e.getValue(), GROUP_KEY.name()).sort(GROUP_KEY.name());
            builder.putMap(e.getKey(), tableSpec);
        }
        return builder.build();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AggregateAllExample()).execute(args);
        System.exit(execute);
    }
}
