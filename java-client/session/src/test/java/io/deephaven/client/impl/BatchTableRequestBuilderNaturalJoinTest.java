//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.NaturalJoinTablesRequest;
import io.deephaven.qst.table.ParentsVisitor;
import io.deephaven.qst.table.TableSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The join type is part of the natural join's identity, so it must survive the trip to the server; an unset
 * {@code join_type} is read as {@code JOIN_TYPE_NOT_SPECIFIED} and silently treated as
 * {@link NaturalJoinType#ERROR_ON_DUPLICATE}.
 */
public class BatchTableRequestBuilderNaturalJoinTest {

    @Test
    void errorOnDuplicate() {
        check(NaturalJoinType.ERROR_ON_DUPLICATE, NaturalJoinTablesRequest.JoinType.ERROR_ON_DUPLICATE);
    }

    @Test
    void firstMatch() {
        check(NaturalJoinType.FIRST_MATCH, NaturalJoinTablesRequest.JoinType.FIRST_MATCH);
    }

    @Test
    void lastMatch() {
        check(NaturalJoinType.LAST_MATCH, NaturalJoinTablesRequest.JoinType.LAST_MATCH);
    }

    @Test
    void exactlyOneMatch() {
        check(NaturalJoinType.EXACTLY_ONE_MATCH, NaturalJoinTablesRequest.JoinType.EXACTLY_ONE_MATCH);
    }

    private static void check(final NaturalJoinType joinType,
            final NaturalJoinTablesRequest.JoinType expected) {
        final TableSpec left = TableSpec.empty(10).view("Key=ii", "L=ii");
        final TableSpec right = TableSpec.empty(10).view("Key=ii", "R=ii");
        final TableSpec join = left.naturalJoin(right, "Key", "R", joinType);

        final BatchTableRequest request = BatchTableRequestBuilder.buildNoChecks(
                spec -> OptionalInt.empty(), ParentsVisitor.postOrder(List.of(join)));

        final NaturalJoinTablesRequest naturalJoin = request.getOpsList().stream()
                .filter(BatchTableRequest.Operation::hasNaturalJoin)
                .map(BatchTableRequest.Operation::getNaturalJoin)
                .findFirst()
                .orElseThrow();

        assertThat(naturalJoin.getColumnsToMatchList()).containsExactly("Key");
        assertThat(naturalJoin.getColumnsToAddList()).containsExactly("R");
        assertThat(naturalJoin.getJoinType()).isEqualTo(expected);
    }
}
