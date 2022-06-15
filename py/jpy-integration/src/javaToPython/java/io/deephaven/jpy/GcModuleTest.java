/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GcModuleTest extends PythonTest {
    private GcModule gc;

    @Before
    public void setUp() {
        gc = GcModule.create();
    }

    @After
    public void tearDown() {
        gc.close();
    }

    @Test
    public void ensureBuilt() {
        // just ensure the gc module can be built - we'll be doing more specific reference counting
        // tests elsewhere
    }
}
