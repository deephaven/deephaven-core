/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import static org.junit.Assert.assertEquals;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.lang.QueryScope;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.WorkerPythonEnvironment;
import io.deephaven.jpy.PythonTest;

import java.util.Collections;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test MatchFilters that reference Python lists.
 */
@Ignore // TODO (deephaven-core#734)
public class PythonMatchFilterTest extends PythonTest {

    @Before
    public void setUp() {
        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicInteractiveProcessInitialization(Configuration.getInstance(),
                    PythonMatchFilterTest.class.getCanonicalName(), new StreamLoggerImpl(System.out, LogLevel.INFO));
        }
    }

    @Test
    public void testIntMatch() {
        WorkerPythonEnvironment.DEFAULT.eval("iii = [1, 2, 3]");
        Object iii = WorkerPythonEnvironment.DEFAULT.fetch("iii");

        QueryScope.addParam("iii", iii);
        WhereFilter filter = WhereFilterFactory.getExpression("ival in iii");
        assertEquals(MatchFilter.class, filter.getClass());

        TableDefinition tableDef =
                new TableDefinition(Collections.singletonList(int.class), Collections.singletonList("ival"));
        filter.init(tableDef);
        Object[] values = ((MatchFilter) filter).getValues();
        // System.out.println(Arrays.toString(values));
        assertEquals(1, values[0]);
        assertEquals(2, values[1]);
        assertEquals(3, values[2]);
    }

    @Test
    public void testStrMatch() {
        WorkerPythonEnvironment.DEFAULT.eval("ss = [\"aa\", \"bb\", \"cc\"]");
        Object ss = WorkerPythonEnvironment.DEFAULT.fetch("ss");

        QueryScope.addParam("ss", ss);
        WhereFilter filter = WhereFilterFactory.getExpression("sval in ss");
        assertEquals(MatchFilter.class, filter.getClass());

        TableDefinition tableDef =
                new TableDefinition(Collections.singletonList(String.class), Collections.singletonList("sval"));
        filter.init(tableDef);
        Object[] values = ((MatchFilter) filter).getValues();
        // System.out.println(Arrays.toString(values));
        assertEquals("aa", values[0]);
        assertEquals("bb", values[1]);
        assertEquals("cc", values[2]);
    }
}
