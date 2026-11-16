//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.ReadonlyArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.fu.LazyPromise;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * Functions in the {@code dh} namespace itself. Not a class: it has no constructor and no instance members.
 */
@JsType(name = "dh", namespace = JsPackage.GLOBAL)
public class DhNamespace {
    private DhNamespace() {}

    /**
     * Resolves each of the pending tables, as {@link JsPendingTable#resolve(Object)} does, and gives the tables in the
     * same order once all have arrived. If one fails, the result rejects as that one did, and the others are canceled
     * via {@link JsPendingTable#cancel()}, so nothing is left open: work still running is stopped, a table already
     * delivered is closed, and anything chained on them that has not produced its table yet fails too. Aborting the
     * signal in {@code options} also cancels each pending table and closes any that did complete.
     * <p>
     * Like any use of a pending table, call it in the task that made them, and do not also await or resolve them
     * elsewhere.
     *
     * <pre>
     * const [joined, totals] = await dh.resolveAll([
     *     left.naturalJoin(right, ["Key"]),
     *     left.sumBy(["Key"]),
     * ]);
     * </pre>
     *
     * @param pendings the pending tables to resolve
     * @param options optionally, a signal to abort with
     * @return a promise of the resolved tables, in the order given, which the caller must close
     */
    public static Promise<JsArray<JsResolvedTable>> resolveAll(ReadonlyArray<JsPendingTable<?>> pendings,
            @JsOptional @JsNullable @TsTypeRef(ResolveOptions.class) Object options) {
        JsPendingTable<?>[] each;
        try {
            // Checked before any is resolved, so a call that rejects here takes none of them
            each = new JsPendingTable<?>[pendings.getLength()];
            for (int i = 0; i < each.length; i++) {
                // A plain JS object, even a thenable, is never an instance of the Java interface
                Object element = pendings.getAtAsAny(i);
                if (!(element instanceof JsPendingTable)) {
                    throw new IllegalArgumentException(
                            "resolveAll takes only pending tables, but element " + i + " is " + element);
                }
                each[i] = (JsPendingTable<?>) element;
            }
        } catch (RuntimeException e) {
            return Promise.reject(LazyPromise.toJsError(e));
        }
        JsResolvedTable[] tables = new JsResolvedTable[each.length];
        if (each.length == 0) {
            return Promise.resolve(Js.<JsArray<JsResolvedTable>>uncheckedCast(tables));
        }
        LazyPromise<JsArray<JsResolvedTable>> result = new LazyPromise<>();
        int[] remaining = {each.length};
        boolean[] failed = {false};
        for (int i = 0; i < each.length; i++) {
            int index = i;
            each[i].resolve(options).then(table -> {
                // After a failure, the cancel has already closed it
                if (!failed[0]) {
                    tables[index] = table;
                    if (--remaining[0] == 0) {
                        result.succeed(Js.uncheckedCast(tables));
                    }
                }
                return null;
            }, err -> {
                if (!failed[0]) {
                    failed[0] = true;
                    // Stops those still running and closes those delivered, so nothing is left open
                    for (JsPendingTable<?> pending : each) {
                        pending.cancel();
                    }
                    result.fail(err);
                }
                return null;
            });
        }
        return result.asPromise();
    }
}
