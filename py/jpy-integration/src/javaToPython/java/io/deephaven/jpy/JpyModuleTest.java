//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JpyModuleTest extends PythonTest {
    private JpyModule jpy;

    @Before
    public void setUp() {
        jpy = JpyModule.create();
    }

    @After
    public void tearDown() {
        jpy.close();
    }

    @Test
    public void flags() {
        // just ensure we can get the flags. the jpy module is mostly used as a debug helper module
        // while writing other code.
        jpy.getFlags();
    }
}
