package io.deephaven.modelfarm;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiPredicate;

public class ConditonalModelsTest extends BaseArrayTestCase {
    private TModel m1;
    private TModel m2;
    private Model<EquityFitDataOptionPrices>[] models;
    private BiPredicate<EquityFitDataOptionPrices, Boolean>[] predicates;
    private Map<Long, Boolean> stateMap;
    private ConditionalModels<EquityFitDataOptionPrices, Boolean, Long> cm;

    private static class TModel implements Model<EquityFitDataOptionPrices> {

        @Override
        public void exec(EquityFitDataOptionPrices data) {}
    }

    private static class EquityFitDataOptionPrices {
        private long uid;

        private long getUnderlyingId() {
            return uid;
        }

        private void setUnderlyingId(long uid) {
            this.uid = uid;
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        m1 = new TModel();
        m2 = new TModel();
        // noinspection unchecked
        models = new Model[] {m1, m2};
        // noinspection unchecked
        predicates = new BiPredicate[] {
                (BiPredicate<EquityFitDataOptionPrices, Boolean>) (d, s) -> d.getUnderlyingId() == 3 && s,
                (BiPredicate<EquityFitDataOptionPrices, Boolean>) (d, s) -> s
        };
        stateMap = new HashMap<>();
        cm = new ConditionalModels<>(models, predicates, stateMap, EquityFitDataOptionPrices::getUnderlyingId);
    }

    public void testLock() {
        final EquityFitDataOptionPrices d = new EquityFitDataOptionPrices();
        d.setUnderlyingId(123);
        final Object l1 = cm.getLock(d);
        final Object l2 = cm.getLock(d);

        d.setUnderlyingId(999);
        final Object l3 = cm.getLock(d);

        assertSame(l1, l2);
        assertNotSame(l2, l3);
    }

    public void testIterator() {
        final EquityFitDataOptionPrices d1 = new EquityFitDataOptionPrices();
        d1.setUnderlyingId(1);

        final EquityFitDataOptionPrices d3 = new EquityFitDataOptionPrices();
        d3.setUnderlyingId(3);

        stateMap.put(1L, false);
        stateMap.put(3L, false);

        final Iterator<Model<EquityFitDataOptionPrices>> it1a = cm.iterator(d1);
        assertFalse(it1a.hasNext());

        final Iterator<Model<EquityFitDataOptionPrices>> it3a = cm.iterator(d3);
        assertFalse(it3a.hasNext());

        stateMap.put(1L, true);

        final Iterator<Model<EquityFitDataOptionPrices>> it1b = cm.iterator(d1);
        assertTrue(it1b.hasNext());
        assertEquals(m2, it1b.next());
        assertFalse(it1b.hasNext());

        final Iterator<Model<EquityFitDataOptionPrices>> it3b = cm.iterator(d3);
        assertFalse(it3b.hasNext());

        stateMap.put(1L, false);
        stateMap.put(3L, true);

        final Iterator<Model<EquityFitDataOptionPrices>> it1c = cm.iterator(d1);
        assertFalse(it1c.hasNext());

        final Iterator<Model<EquityFitDataOptionPrices>> it3c = cm.iterator(d3);
        assertTrue(it3c.hasNext());
        assertEquals(m1, it3c.next());
        assertTrue(it3c.hasNext());
        assertEquals(m2, it3c.next());
        assertFalse(it3c.hasNext());
    }
}
