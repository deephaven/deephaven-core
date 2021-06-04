package io.deephaven.qst.table.column.type;

import java.util.Optional;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class GenericType<T> extends ColumnTypeBase<T> {

    public static <T> GenericType<T> of(Class<T> clazz) {
        return ImmutableGenericType.of(clazz);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkClazz() {
        final Optional<ColumnType<T>> staticType = ColumnTypeMappings.findStatic(clazz());
        if (staticType.isPresent()) {
            throw new IllegalArgumentException(String.format("Use static type %s instead", staticType.get()));
        }
    }
}
