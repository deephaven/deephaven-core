/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import java.util.ArrayList;

public interface KeyHolder {
    long getKey();

    public long getPrevKey();

    void setKey(long key);

    void addDependent(KeyHolder dependent);

    void removeDependent(KeyHolder dependent);

    void touch();

    public abstract class Base implements KeyHolder {
        protected ArrayList<KeyHolder> dependents;

        public void addDependent(KeyHolder dependent) {
            if (dependents == null) {
                dependents = new ArrayList<KeyHolder>(2);
            }
            dependents.add(dependent);
        }

        public void removeDependent(KeyHolder dependent) {
            dependents.remove(dependent);
        }

        public void touch() {
            if (dependents != null) {
                for (KeyHolder dependent : dependents) {
                    dependent.touch();
                }
            }
        }
    }

}
