/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.data;

import java.io.Serializable;

public class RollupDefinition implements Serializable {
    public enum LeafType {
        Normal, Constituent
    }

    private LeafType leafType;

    public LeafType getLeafType() {
        return leafType;
    }

}
