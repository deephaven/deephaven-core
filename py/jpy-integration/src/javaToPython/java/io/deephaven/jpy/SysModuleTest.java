//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SysModuleTest extends PythonTest {
    private SysModule sys;

    @Before
    public void setUp() {
        sys = SysModule.create();
    }

    @After
    public void tearDown() {
        sys.close();
    }

    @Test
    public void ensureBuilt() {
        // just ensure the sys module can be built - we'll be doing more specific reference counting
        // tests elsewhere
    }
}
