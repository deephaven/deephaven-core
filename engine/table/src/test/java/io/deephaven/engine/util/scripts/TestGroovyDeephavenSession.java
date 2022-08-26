/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util.scripts;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestGroovyDeephavenSession {

    private LivenessScope livenessScope;
    private GroovyDeephavenSession session;

    @Before
    public void setup() throws IOException {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        session = new GroovyDeephavenSession(NoOp.INSTANCE, null, GroovyDeephavenSession.RunScripts.none());
    }

    @After
    public void teardown() {
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    public <T> T fetch(final String name, final Class<T> clazz) {
        // note var is guaranteed to be non-null
        final Object var = session.getVariable(name);
        if (clazz.isAssignableFrom(var.getClass())) {
            // noinspection unchecked
            return (T) var;
        }
        throw new RuntimeException("Unexpected type for variable '" + name + "'. Found: "
                + var.getClass().getCanonicalName() + " Expected: " + clazz.getCanonicalName());
    }

    public Table fetchTable(final String name) {
        return fetch(name, Table.class);
    }

    @Test
    public void testNullCast() {
        session.evaluateScript("x = null; y = emptyTable(0).update(\"X = (java.util.List)x\")");
        final Table y = fetchTable("y");
        final TableDefinition definition = y.getDefinition();
        final Class<?> colClass = definition.getColumn("X").getDataType();
        Assert.equals(colClass, "colClass", java.util.List.class);
    }

    @Test
    public void testScriptDefinedClass() {
        session.evaluateScript("class MyObj {\n" +
                "    public int a;\n" +
                "    MyObj(int a) {\n" +
                "        this.a = a\n" +
                "    }\n" +
                "}\n" +
                "obj = new MyObj(1)\n" +
                "result = emptyTable(1).select(\"A = obj.a\")");
        Assert.neqNull(fetch("obj", Object.class), "fetchObject");
        final Table result = fetchTable("result");
        Assert.eqFalse(result.isFailed(), "result.isFailed()");
    }

    @Test
    public void testScriptResultOrder() {
        final ScriptSession.Changes changes = session.evaluateScript("x=emptyTable(10)\n" +
                "z=emptyTable(10)\n" +
                "y=emptyTable(10)\n" +
                "u=emptyTable(10)");
        final String[] names = new String[] {"x", "z", "y", "u"};
        final MutableInt offset = new MutableInt();
        changes.created.forEach((name, type) -> {
            Assert.eq(name, "name", names[offset.getAndIncrement()], "names[offset.getAndIncrement()]");
        });
    }
}

