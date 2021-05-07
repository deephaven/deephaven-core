package io.deephaven.web.shared.data.columns;

import java.io.Serializable;

public abstract class ColumnData implements Serializable {

    public abstract Object getData();
}
