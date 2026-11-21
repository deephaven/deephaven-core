//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationOptions;
import io.deephaven.api.agg.StateReclamation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregateTableTest {

    private static final List<Aggregation> SUM = List.of(Aggregation.AggSum("x"));
    private static final List<ColumnName> KEY = List.of(ColumnName.of("Key"));

    @Test
    void optionsBecomeTheAggregateTable() {
        final TableSpec initialGroups = TableSpec.empty(2).view("Key=i");
        final AggregationOptions<TableSpec> options = AggregationOptions.<TableSpec>builder()
                .preserveEmpty(true)
                .initialGroups(initialGroups)
                .stateReclamation(StateReclamation.none())
                .build();
        final AggregateTable aggregated = (AggregateTable) TableSpec.empty(1).view("Key=i", "x=i")
                .aggBy(SUM, KEY, options);
        assertThat(aggregated.preserveEmpty()).isTrue();
        assertThat(aggregated.initialGroups()).hasValue(initialGroups);
        assertThat(aggregated.stateReclamation()).isEqualTo(StateReclamation.none());
        assertThat(aggregated.options()).isEqualTo(options);
    }

    @Test
    void theOlderOverloadUsesTheConfiguredReclamation() {
        final AggregateTable aggregated = (AggregateTable) TableSpec.empty(1).view("Key=i", "x=i")
                .aggBy(SUM, false, null, KEY);
        assertThat(aggregated.stateReclamation()).isEqualTo(StateReclamation.configured());
        assertThat(aggregated.options()).isEqualTo(AggregationOptions.<TableSpec>defaults());
    }
}
