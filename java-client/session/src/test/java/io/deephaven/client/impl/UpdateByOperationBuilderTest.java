//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByOperation.Visitor;
import io.deephaven.api.updateby.spec.CumSumSpec;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByCumulativeSum;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateByOperationBuilderTest {

    @Test
    void ColumnUpdateOperation() {
        check(ColumnUpdateOperation.builder().spec(CumSumSpec.of()).addColumns(ColumnName.of("Foo")).build());
    }

    private enum ExpectedOperationVisitor implements Visitor<UpdateByRequest.UpdateByOperation> {
        INSTANCE;

        // Note: this is written in a way to encourage new tests get added any time a new UpdateByOperation type gets
        // created. The visitor methods should not typically need to look at the actual message - it's meant to return
        // the expected value.

        @Override
        public UpdateByRequest.UpdateByOperation visit(ColumnUpdateOperation clause) {
            return UpdateByRequest.UpdateByOperation.newBuilder()
                    .setColumn(UpdateByColumn.newBuilder()
                            .setSpec(UpdateBySpec.newBuilder().setSum(UpdateByCumulativeSum.getDefaultInstance())
                                    .build())
                            .addMatchPairs("Foo").build())
                    .build();
        }
    }

    private static void check(UpdateByOperation spec) {
        check(spec, spec.walk(ExpectedOperationVisitor.INSTANCE));
    }

    private static void check(UpdateByOperation spec, UpdateByRequest.UpdateByOperation expected) {
        assertThat(UpdateByBuilder.adapt(spec)).isEqualTo(expected);
    }
}
