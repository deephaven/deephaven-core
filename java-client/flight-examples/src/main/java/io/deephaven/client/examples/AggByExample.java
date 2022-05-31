package io.deephaven.client.examples;

import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.LabeledTables.Builder;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "aggby-examples", mixinStandardHelpOptions = true,
        description = "Agg by examples", version = "0.1.0")
class AggByExample extends AggInputBase {

    @Override
    public LabeledTables labeledTables(TableSpec base) {
        final Builder builder = LabeledTables.builder();
        builder.putMap("countBy", base.countBy("Count", GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("selectDistinct", base.selectDistinct(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("firstBy", base.firstBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("lastBy", base.lastBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("minBy", base.minBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("maxBy", base.maxBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("sumBy", base.sumBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("avgBy", base.avgBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("medianBy", base.medianBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("stdBy", base.stdBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("varBy", base.varBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        builder.putMap("absSumBy", base.sumBy(GROUP_KEY.name()).sort(GROUP_KEY.name()));
        return builder.build();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new AggByExample()).execute(args);
        System.exit(execute);
    }
}
