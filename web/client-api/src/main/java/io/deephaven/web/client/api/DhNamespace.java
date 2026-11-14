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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Functions in the {@code dh} namespace itself. Not a class: it has no constructor and no instance members.
 */
@JsType(name = "dh", namespace = JsPackage.GLOBAL)
public class DhNamespace {
    private DhNamespace() {}

    /**
     * Resolves each of the pending tables, as {@link JsPendingTable#resolve(Object)} does, and gives the tables in the
     * same order once all have arrived. If one fails, the result rejects as that one did, and the tables that arrived,
     * or arrive later, are closed, so nothing is left open. Aborting the signal in {@code options} cancels each pending
     * table not yet delivered, so the result rejects and closes the rest.
     * <p>
     * Like any use of a pending table, call it in the task that made them. A pending table that is also resolved or
     * awaited elsewhere shares its table with this call, which closes it on failure.
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
        // The same pending table given twice resolves to one table, closed once
        List<JsResolvedTable> closed = new ArrayList<>();
        Consumer<JsResolvedTable> close = table -> {
            if (!closed.contains(table)) {
                closed.add(table);
                table.close();
            }
        };
        for (int i = 0; i < each.length; i++) {
            int index = i;
            each[i].resolve(options).then(table -> {
                if (failed[0]) {
                    // Arrived after another failed, so nobody else will close it
                    close.accept(table);
                } else {
                    tables[index] = table;
                    if (--remaining[0] == 0) {
                        result.succeed(Js.uncheckedCast(tables));
                    }
                }
                return null;
            }, err -> {
                if (!failed[0]) {
                    failed[0] = true;
                    for (JsResolvedTable table : tables) {
                        if (table != null) {
                            close.accept(table);
                        }
                    }
                    result.fail(err);
                }
                return null;
            });
        }
        return result.asPromise();
    }
}
