/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stats;

import com.google.monitoring.runtime.instrumentation.AllocationRecorder;
import com.google.monitoring.runtime.instrumentation.Sampler;
import io.deephaven.hash.KeyedObjectHash;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.stats.State;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;

/**
 * Use the allocation instrumenter from http://code.google.com/p/java-allocation-instrumenter/ to produce stats
 *
 * To use make sure you set
 *
 * -j -Dallocation.stats.enabled=true -j -Xbootclasspath/a:java_lib/allocation-2.0.jar -j
 * -javaagent:java_lib/allocation-2.0.jar (maybe even use -j -noverify)
 */
public class ObjectAllocationCollector {

    public static boolean DUMP_STACK = false;
    public static String[] CLASS_NAMES;
    static {
        String sClassNames = System.getProperty("ObjectAllocationCollector.dumpStackForClasses");
        if (null != sClassNames) {
            CLASS_NAMES = sClassNames.split(",");
            for (int nIndex = 0, nLength = CLASS_NAMES.length; nIndex < nLength; nIndex++) {
                CLASS_NAMES[nIndex] = CLASS_NAMES[nIndex].intern();
            }
        } else {
            CLASS_NAMES = new String[0];
        }

        String sEnable = System.getProperty("ObjectAllocationCollector.dumpStack");
        if (null != sEnable && (sEnable.toLowerCase().contains("y") || sEnable.toLowerCase().contains("t"))) {
            DUMP_STACK = true;
        }
    }

    private static class AllocationState {
        private final Value size;
        private final Class clazz;
        private Boolean dumpStack;

        private AllocationState(final Value size, final Class clazz) {
            this.size = size;
            this.clazz = clazz;
        }

        // public AllocationState(Class clazz) {
        // this.clazz = clazz;
        // if (!clazz.getName().startsWith("sun.")) {
        // size = Stats.makeItem("GAllocation", clazz.getName(), State.FACTORY).getValue();
        // } else {
        // size = null;
        // }
        // }

        public static KeyedObjectKey<Class, AllocationState> keyDef = new KeyedObjectKey<Class, AllocationState>() {
            @Override
            protected KeyedObjectKey clone() throws CloneNotSupportedException {
                return (KeyedObjectKey) super.clone();
            }

            public Class getKey(AllocationState v) {
                return v.clazz;
            }

            public int hashKey(Class k) {
                return k.hashCode();
            }

            public boolean equalKey(Class k, AllocationState v) {
                return k.equals(v.clazz);
            }
        };

        public void sample(long bytes) {
            if (size != null) {
                size.sample(bytes);
            }
            if (null == dumpStack) {
                if (null != CLASS_NAMES) {
                    for (String sClassName : CLASS_NAMES) {
                        if (sClassName == clazz.getName()) {
                            dumpStack = Boolean.TRUE;
                            return;
                        }
                    }
                    dumpStack = Boolean.FALSE;
                }
                return;
            }
            if (DUMP_STACK && dumpStack) {
                new Throwable().printStackTrace();
            }
        }
    }

    private final KeyedObjectHash<Class, AllocationState> classAllocationStates =
            new KeyedObjectHash<Class, AllocationState>(AllocationState.keyDef);
    private final KeyedObjectHash.ValueFactoryT<Class, AllocationState, Value> STATE_FACTORY =
            new KeyedObjectHash.ValueFactoryT<Class, AllocationState, Value>() {
                public AllocationState newValue(Class key, Value value) {
                    return new AllocationState(value, key);
                }
            };

    public ObjectAllocationCollector() {
        // This hooks the JVM bytecode to call us back every time an object is allocated
        AllocationRecorder.addSampler(new Sampler() {
            public void sampleAllocation(int count, String desc,
                    Object newObj, long size) {
                // unfortunately, we can't use this b/c it syncs around the putIfAbsent AND the Stats.makeItem which
                // causes deadlocks!
                // classAllocationStates.putIfAbsent(newObj.getClass(), STATE_FACTORY).sample(size);

                final Class clazz = newObj.getClass();
                if (clazz.getName().startsWith("sun.") || clazz.getName().endsWith("AllocationState"))
                    return;

                final AllocationState allocationState = classAllocationStates.get(clazz);
                if (allocationState == null) {
                    final Value value = Stats.makeItem("GAllocation", clazz.getName(), State.FACTORY).getValue();
                    final AllocationState state = new AllocationState(value, clazz);
                    final AllocationState existing = classAllocationStates.putIfAbsent(clazz, state);
                    (existing == null ? state : existing).sample(size);
                } else {
                    allocationState.sample(size);
                }
            }
        });
    }
}
