/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import junit.framework.TestCase;

public class RAPriQueueTest extends TestCase {

    class Element {
        int value;
        int qpos = 0;

        public String toString() {
            return Integer.toString(value);
        }
    }

    class Adapter implements RAPriQueue.Adapter<Element> {

        public boolean less(Element a, Element b) {
            return a.value < b.value;
        }

        public void setPos(Element el, int pos) {
            el.qpos = pos;
        }

        public int getPos(Element el) {
            return el.qpos;
        }
    }

    public void testEnqueueDequeue() {
        Element[] a = new Element[10];
        for (int i = 0; i < a.length; ++i) {
            a[i] = new Element();
        }
        RAPriQueue<Element> pq = new RAPriQueue<Element>(3, new Adapter(), Element.class);

        a[0].value = 1;
        pq.enter(a[0]);
        assertTrue(pq.testInvariant(""));
        a[1].value = 9;
        pq.enter(a[1]);
        assertTrue(pq.testInvariant(""));
        a[2].value = 8;
        pq.enter(a[2]);
        assertTrue(pq.testInvariant(""));
        a[3].value = 5;
        pq.enter(a[3]);
        assertTrue(pq.testInvariant(""));
        a[4].value = 2;
        pq.enter(a[4]);
        assertTrue(pq.testInvariant(""));
        a[5].value = 3;
        pq.enter(a[5]);
        assertTrue(pq.testInvariant(""));
        a[6].value = 6;
        pq.enter(a[6]);
        assertTrue(pq.testInvariant(""));
        a[7].value = 4;
        pq.enter(a[7]);
        assertTrue(pq.testInvariant(""));
        a[8].value = 7;
        pq.enter(a[8]);
        assertTrue(pq.testInvariant(""));
        a[9].value = 10;
        pq.enter(a[9]);
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.top() == a[0]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[4]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[5]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[7]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[3]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[6]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[8]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[2]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[1]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[9]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.isEmpty());
    }

    public void testRandomRemove() {
        Element[] a = new Element[10];
        for (int i = 0; i < a.length; ++i) {
            a[i] = new Element();
        }
        RAPriQueue<Element> pq = new RAPriQueue<Element>(3, new Adapter(), Element.class);

        a[0].value = 1;
        pq.enter(a[0]);
        assertTrue(pq.testInvariant(""));
        a[1].value = 9;
        pq.enter(a[1]);
        assertTrue(pq.testInvariant(""));
        a[2].value = 8;
        pq.enter(a[2]);
        assertTrue(pq.testInvariant(""));
        a[3].value = 5;
        pq.enter(a[3]);
        assertTrue(pq.testInvariant(""));
        a[4].value = 2;
        pq.enter(a[4]);
        assertTrue(pq.testInvariant(""));
        a[5].value = 3;
        pq.enter(a[5]);
        assertTrue(pq.testInvariant(""));
        a[6].value = 6;
        pq.enter(a[6]);
        assertTrue(pq.testInvariant(""));
        a[7].value = 4;
        pq.enter(a[7]);
        assertTrue(pq.testInvariant(""));
        a[8].value = 7;
        pq.enter(a[8]);
        assertTrue(pq.testInvariant(""));
        a[9].value = 10;
        pq.enter(a[9]);
        assertTrue(pq.testInvariant(""));

        pq.remove(a[1]);
        assertTrue(pq.testInvariant(""));
        pq.remove(a[3]);
        assertTrue(pq.testInvariant(""));
        pq.remove(a[5]);
        assertTrue(pq.testInvariant(""));
        pq.remove(a[7]);
        assertTrue(pq.testInvariant(""));
        pq.remove(a[9]);
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.top() == a[0]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[4]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[6]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[8]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[2]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.isEmpty());
    }

    public void testReversal() {
        Element[] a = new Element[10];
        for (int i = 0; i < a.length; ++i) {
            a[i] = new Element();
        }
        RAPriQueue<Element> pq = new RAPriQueue<Element>(3, new Adapter(), Element.class);

        a[0].value = 1;
        pq.enter(a[0]);
        assertTrue(pq.testInvariant(""));
        a[1].value = 9;
        pq.enter(a[1]);
        assertTrue(pq.testInvariant(""));
        a[2].value = 8;
        pq.enter(a[2]);
        assertTrue(pq.testInvariant(""));
        a[3].value = 5;
        pq.enter(a[3]);
        assertTrue(pq.testInvariant(""));
        a[4].value = 2;
        pq.enter(a[4]);
        assertTrue(pq.testInvariant(""));
        a[5].value = 3;
        pq.enter(a[5]);
        assertTrue(pq.testInvariant(""));
        a[6].value = 6;
        pq.enter(a[6]);
        assertTrue(pq.testInvariant(""));
        a[7].value = 4;
        pq.enter(a[7]);
        assertTrue(pq.testInvariant(""));
        a[8].value = 7;
        pq.enter(a[8]);
        assertTrue(pq.testInvariant(""));
        a[9].value = 10;
        pq.enter(a[9]);
        assertTrue(pq.testInvariant(""));

        a[0].value = 11 - a[0].value;
        pq.enter(a[0]);
        assertTrue(pq.testInvariant(""));
        a[1].value = 11 - a[1].value;
        pq.enter(a[1]);
        assertTrue(pq.testInvariant(""));
        a[2].value = 11 - a[2].value;
        pq.enter(a[2]);
        assertTrue(pq.testInvariant(""));
        a[3].value = 11 - a[3].value;
        pq.enter(a[3]);
        assertTrue(pq.testInvariant(""));
        a[4].value = 11 - a[4].value;
        pq.enter(a[4]);
        assertTrue(pq.testInvariant(""));
        a[5].value = 11 - a[5].value;
        pq.enter(a[5]);
        assertTrue(pq.testInvariant(""));
        a[6].value = 11 - a[6].value;
        pq.enter(a[6]);
        assertTrue(pq.testInvariant(""));
        a[7].value = 11 - a[7].value;
        pq.enter(a[7]);
        assertTrue(pq.testInvariant(""));
        a[8].value = 11 - a[8].value;
        pq.enter(a[8]);
        assertTrue(pq.testInvariant(""));
        a[9].value = 11 - a[9].value;
        pq.enter(a[9]);
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.top() == a[9]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[1]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[2]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[8]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[6]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[3]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[7]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[5]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[4]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));
        assertTrue(pq.top() == a[0]);
        pq.removeTop();
        assertTrue(pq.testInvariant(""));

        assertTrue(pq.isEmpty());
    }
}
