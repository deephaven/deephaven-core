package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.PartitionedTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * {@link PartitionedTable.Proxy} {@link java.lang.reflect.InvocationHandler} implementation.
 */
public class PartitionedTableProxyHandler extends LivenessArtifact implements InvocationHandler {

    /**
     * The underlying target {@link PartitionedTable}.
     */
    private final PartitionedTable target;

    private PartitionedTableProxyHandler(@NotNull final PartitionedTable target) {
        this.target = target;
    }

    /**
     * Make a {@link PartitionedTable.Proxy proxy} to the supplied {@code target}.
     *
     * @param target The target {@link PartitionedTable} to whose constituents should be used when proxying
     *        {@link io.deephaven.api.TableOperations table operations}
     * @return A {@link PartitionedTable.Proxy proxy} to {@code target}
     */
    public static PartitionedTable.Proxy proxyFor(@NotNull final PartitionedTable target) {
        return (PartitionedTable.Proxy) Proxy.newProxyInstance(PartitionedTableProxyHandler.class.getClassLoader(),
                new Class[] {PartitionedTable.Proxy.class},
                new PartitionedTableProxyHandler(target));
    }

    @Override
    public Object invoke(
            @NotNull final Object proxy,
            @NotNull final Method method,
            @Nullable final Object[] args) throws Throwable {
        return null;
    }
}
