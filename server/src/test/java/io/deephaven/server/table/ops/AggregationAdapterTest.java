package io.deephaven.server.table.ops;

import io.deephaven.proto.backplane.grpc.Aggregation.TypeCase;
import io.deephaven.server.table.ops.AggregationAdapter.Singleton;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class AggregationAdapterTest {

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
}
