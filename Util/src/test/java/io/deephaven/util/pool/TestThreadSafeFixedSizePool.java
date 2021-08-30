/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.pool;

import io.deephaven.base.MockFactory;
import io.deephaven.base.pool.MockClearingProcedure;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.verify.RequirementFailure;
import junit.framework.TestCase;

// --------------------------------------------------------------------
/**
 * Tests for {@link io.deephaven.base.pool.ThreadSafeFixedSizePool}.
 */
public class TestThreadSafeFixedSizePool extends TestCase {

    private MockFactory<Object> m_mockObjectFactory;
    private MockClearingProcedure<Object> m_mockClearingProcedure;

    // this size specially chosen to exactly fill the (smallest) ring buffer
    private static final Object[] OBJECTS = {
            "alpha", "bravo", "charlie", "delta",
            "echo", "foxtrot", "golf", "hotel",
            "igloo", "juliet", "kilo", "lima",
            "mike", "november"
    };

    // ----------------------------------------------------------------
    public void testThreadSafeFixedSizePool() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();


        // create pool
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        Pool<Object> pool = io.deephaven.base.pool.ThreadSafeFixedSizePool.FACTORY.create(OBJECTS.length,
                m_mockObjectFactory, m_mockClearingProcedure);
        assertEquals("call()call()call()call()call()call()call()call()call()call()call()call()call()call()",
                m_mockObjectFactory.getActivityRecordAndReset());

        // take
        Object alphaObject = OBJECTS[0];
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // give
        pool.give(alphaObject);
        assertEquals("call(alpha)", m_mockClearingProcedure.getActivityRecordAndReset());
        checkNoOtherActivity();

        // (give on full pool would block)

        // give null
        pool.give(null);
        checkNoOtherActivity();

        // take from pool
        for (int nIndex = 1; nIndex < OBJECTS.length; nIndex++) {
            assertSame(OBJECTS[nIndex], pool.take());
            checkNoOtherActivity();
        }

        // take from pool
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // (take on empty pool would block)

        // give
        for (Object object : OBJECTS) {
            pool.give(object);
            assertEquals("call(" + object + ")", m_mockClearingProcedure.getActivityRecordAndReset());
            checkNoOtherActivity();
        }

        // take from pool
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();
    }

    // ----------------------------------------------------------------
    public void testThreadSafeFixedSizePoolNoClearingProcedure() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();

        // create pool
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        Pool<Object> pool = io.deephaven.base.pool.ThreadSafeFixedSizePool.FACTORY.create(OBJECTS.length,
                m_mockObjectFactory, null);
        assertEquals("call()call()call()call()call()call()call()call()call()call()call()call()call()call()",
                m_mockObjectFactory.getActivityRecordAndReset());

        // take
        Object alphaObject = OBJECTS[0];
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // give
        pool.give(alphaObject);
        checkNoOtherActivity();

        // (give on full pool would block)

        // give null
        pool.give(null);
        checkNoOtherActivity();
    }

    // ----------------------------------------------------------------
    public void testThreadSafeFixedSizePoolNoFactory() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();

        // no factory
        try {
            new io.deephaven.base.pool.ThreadSafeFixedSizePool<Object>(OBJECTS.length, null, m_mockClearingProcedure);
            fail("Should have thrown");
        } catch (RequirementFailure requirementFailure) {
            // expected
        }

        // too small
        try {
            new io.deephaven.base.pool.ThreadSafeFixedSizePool<Object>(6, m_mockObjectFactory, m_mockClearingProcedure);
            fail("should have thrown");
        } catch (RequirementFailure requirementFailure) {
            // expected
        }

        // minimum size
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        new io.deephaven.base.pool.ThreadSafeFixedSizePool<Object>(7, m_mockObjectFactory, null);
        assertEquals("call()call()call()call()call()call()call()", m_mockObjectFactory.getActivityRecordAndReset());

        // no factory
        try {
            io.deephaven.base.pool.ThreadSafeFixedSizePool.FACTORY.create(OBJECTS.length, null,
                    m_mockClearingProcedure);
            fail("should have thrown");
        } catch (RequirementFailure requirementFailure) {
            // expected
        }

        // too small
        try {
            io.deephaven.base.pool.ThreadSafeFixedSizePool.FACTORY.create(6, m_mockObjectFactory,
                    m_mockClearingProcedure);
            fail("should have thrown");
        } catch (RequirementFailure requirementFailure) {
            // expected
        }

        // minimum size
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        new ThreadSafeFixedSizePool<>(7, m_mockObjectFactory, null);
        assertEquals("call()call()call()call()call()call()call()", m_mockObjectFactory.getActivityRecordAndReset());
    }

    // ----------------------------------------------------------------
    private void checkNoOtherActivity() {
        assertEquals("", m_mockObjectFactory.getActivityRecordAndReset());
        assertEquals("", m_mockClearingProcedure.getActivityRecordAndReset());
    }
}
