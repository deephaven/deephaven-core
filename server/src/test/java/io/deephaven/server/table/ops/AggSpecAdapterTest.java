//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecAvg;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggSpec.TypeCase;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.server.table.ops.AggSpecAdapter.Singleton;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class AggSpecAdapterTest {

    public static final AggSpec AGG_SPEC_MISSING_TYPE = AggSpec.getDefaultInstance();

    public static final AggSpec AGG_SPEC_AVG = AggSpec.newBuilder().setAvg(AggSpecAvg.getDefaultInstance()).build();

    public static final AggSpec AGG_SPEC_HAS_UNKNOWN = AggSpec.newBuilder(AGG_SPEC_AVG)
            .setUnknownFields(UnknownFieldSet.newBuilder()
                    .addField(9999, Field.newBuilder().addFixed32(32).build())
                    .build())
            .build();

    public static final AggSpecNonUniqueSentinel EMPTY_SENTINEL = AggSpecNonUniqueSentinel.getDefaultInstance();

    public static final AggSpecNonUniqueSentinel NULL_SENTINEL =
            AggSpecNonUniqueSentinel.newBuilder().setNullValue(NullValue.NULL_VALUE).build();

    public static final AggSpecUnique AGG_SPEC_UNIQUE_MISSING_SENTINEL = AggSpecUnique.getDefaultInstance();

    public static final AggSpecUnique AGG_SPEC_UNIQUE_NULL_SENTINEL = AggSpecUnique.newBuilder()
            .setNonUniqueSentinel(NULL_SENTINEL)
            .build();


    @Test
    public void allTypeCasesHandled() {
        final Set<TypeCase> validTypeCases = Arrays.stream(TypeCase.values())
                .filter(x -> x != TypeCase.TYPE_NOT_SET)
                .collect(Collectors.toSet());

        final Set<TypeCase> actualCases = Stream.concat(
                Singleton.INSTANCE.adapters().adapters.keySet().stream(),
                Singleton.INSTANCE.adapters().unimplemented.stream())
                .collect(Collectors.toSet());

        assertThat(actualCases).isEqualTo(validTypeCases);
    }

    @Test
    public void validateAggSpecHasType() {
        try {
            AggSpecAdapter.validate(AGG_SPEC_MISSING_TYPE);
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e).hasMessageContaining("io.deephaven.proto.backplane.grpc.AggSpec must have oneof type");
        }
    }

    @Test
    public void validateAggSpecNoUnknown() {
        try {
            AggSpecAdapter.validate(AGG_SPEC_HAS_UNKNOWN);
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e).hasMessageContaining("io.deephaven.proto.backplane.grpc.AggSpec has unknown field(s)");
        }
    }

    @Test
    public void validateAggSpec() {
        AggSpecAdapter.validate(AGG_SPEC_AVG);
    }

    @Test
    public void validateAggSpecUniqueHasSentinel() {
        try {
            AggSpecAdapter.validate(AGG_SPEC_UNIQUE_MISSING_SENTINEL);
            failBecauseExceptionWasNotThrown(StatusRuntimeException.class);
        } catch (StatusRuntimeException e) {
            assertThat(e).hasMessageContaining(
                    "io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique must have field non_unique_sentinel (2)");
        }
    }

    @Test
    public void validateAggSpecUnique() {
        AggSpecAdapter.validate(AGG_SPEC_UNIQUE_NULL_SENTINEL);
    }

    @Test
    public void validateNonUniqueSentinelHasType() {
        try {
            AggSpecAdapter.validate(EMPTY_SENTINEL);
        } catch (StatusRuntimeException e) {
            assertThat(e).hasMessageContaining(
                    "io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel must have oneof type");
        }
    }

    @Test
    public void validateNullSentinel() {
        AggSpecAdapter.validate(NULL_SENTINEL);
    }
}
