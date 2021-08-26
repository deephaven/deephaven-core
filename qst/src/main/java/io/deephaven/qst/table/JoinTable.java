package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Collection;

/**
 * @see TableOperations#join(Object, Collection, Collection, int)
 */
@Immutable
@NodeStyle
public abstract class JoinTable extends JoinBase {

    /**
     * The number of {@link #reserveBits() reserve bits} to use when it is not explicitly set during building.
     *
     * <p>
     * By default, is 10. Can be changed with system property {@code JoinTable.reserveBits}.
     */
    public static final int DEFAULT_RESERVE_BITS = Integer.getInteger("JoinTable.reserveBits", 10);

    public static Builder builder() {
        return ImmutableJoinTable.builder();
    }

    @Default
    public int reserveBits() {
        return DEFAULT_RESERVE_BITS;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends Join.Builder<JoinTable, Builder> {

        Builder reserveBits(int reserveBits);
    }
}
