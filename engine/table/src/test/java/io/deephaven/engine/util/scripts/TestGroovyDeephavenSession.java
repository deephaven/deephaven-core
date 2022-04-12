/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.scripts;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.FormulaCompilationException;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
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
        session = new GroovyDeephavenSession(NoOp.INSTANCE, null, GroovyDeephavenSession.RunScripts.none(), false);
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
    public void testAnonymousObject() {
        final String script = "x = new Object() {\n" +
                "  long get(long ii) { return ii; }\n" +
                "}\n" +
                "y = emptyTable(1).update(\"X = x[ii]\")";
        try {
            session.evaluateScript(script);
        } catch (FormulaCompilationException exception) {
            Assert.eqTrue(exception.getCause().getCause().getMessage().contains(
                    "Cannot find method get(long) in interface groovy.lang.GroovyObject"),
                    "exception contains helpful error message");
        }
    }
}

