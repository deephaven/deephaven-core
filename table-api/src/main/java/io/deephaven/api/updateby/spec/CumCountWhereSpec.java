//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.Filter;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * An {@link UpdateBySpec} for performing a windowed rolling formula operation.
 */
@Immutable
@SimpleStyle
public abstract class CumCountWhereSpec extends UpdateBySpecBase {
    @Value.Parameter
    public abstract ColumnName column();

    @Value.Parameter
    public abstract Filter filter();

    public static CumCountWhereSpec of(String resultColumn, String... filters) {
        return of(resultColumn, Filter.and(Filter.from(filters)));
    }

    public static CumCountWhereSpec of(String resultColumn, Filter filter) {
        return ImmutableCumCountWhereSpec.of(ColumnName.of(resultColumn), filter);
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
