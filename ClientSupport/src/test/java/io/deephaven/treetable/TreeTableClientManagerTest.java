/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.testutil.QueryTableTestBase;
import org.jmock.AbstractExpectations;

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class TreeTableClientManagerTest extends QueryTableTestBase {
    private TreeTableClientTableManager.Client[] clients;
    private SnapshotState mockSnapshotState;
    private final ExecutorService pool = Executors.newFixedThreadPool(1);

    static class DelayingReleaseProxy implements InvocationHandler {
        private static Method RELEASE_METHOD;
        private static Method IS_REFRESHING;
        private static Method TRY_RETAIN;
        private static Method GET_WEAK_REFERENCE;

        static {
            try {
                RELEASE_METHOD = LivenessReferent.class.getMethod("dropReference");
                TRY_RETAIN = LivenessReferent.class.getMethod("tryRetainReference");
                IS_REFRESHING = Table.class.getMethod("isRefreshing");
                GET_WEAK_REFERENCE = LivenessReferent.class.getMethod("getWeakReference");
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws InterruptedException {
            if (method.equals(RELEASE_METHOD)) {
                // Sleep for a bit so we can generate CMEs
                Thread.sleep(250);
            } else if (method.equals(IS_REFRESHING)) {
                return true;
            } else if (method.equals(TRY_RETAIN)) {
                return true;
            } else if (method.equals(GET_WEAK_REFERENCE)) {
                return new WeakReference(proxy);
            }
            return null;
        }
    }

    private Table makeProxy() {
        return (Table) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {Table.class},
                new DelayingReleaseProxy());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        clients = new TreeTableClientTableManager.Client[5];
        for (int i = 0; i < 5; i++) {
            clients[i] = mock(TreeTableClientTableManager.Client.class, "CLIENT_" + i);
            final int myI = i;
            checking(new Expectations() {
                {
                    allowing(clients[myI]).addDisconnectHandler(
                            with(AbstractExpectations.<Consumer<TreeTableClientTableManager.Client>>anything()));
                    allowing(clients[myI]).removeDisconnectHandler(
                            with(AbstractExpectations.<Consumer<TreeTableClientTableManager.Client>>anything()));
                }
            });
        }

        mockSnapshotState = mock(SnapshotState.class);
    }

    /**
     * This method tests for regression of the ConcurrentModificationException documented by IDS-5134
     */
    public void testIds5134CME() throws ExecutionException, InterruptedException {
        final TreeTableClientTableManager.ClientState stateObj = TreeTableClientTableManager.DEFAULT.get(clients[0]);
        final TreeTableClientTableManager.TreeState treeState00 = stateObj.getTreeState(0, () -> mockSnapshotState);
        assertSame(mockSnapshotState, treeState00.getUserState());

        // Retain a few tables
        final Table[] proxies = IntStream.range(0, 10).mapToObj((i) -> makeProxy()).toArray(Table[]::new);
        for (int i = 0; i < 5; i++) {
            treeState00.retain(i, proxies[i]);
        }

        Future bacon = pool.submit(treeState00::releaseAll);
        for (int i = 5; i < 10; i++) {
            treeState00.retain(i, proxies[i]);
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        bacon.get();
    }
}
