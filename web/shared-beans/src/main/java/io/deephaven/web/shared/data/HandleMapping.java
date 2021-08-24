package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * Used to collect various handles that are always needed whenever deriving a new table.
 */
public class HandleMapping implements Serializable {

    private TableHandle source;
    private TableHandle newId;

    public HandleMapping() {}

    public HandleMapping(TableHandle source, TableHandle newId) {
        this.source = source;
        this.newId = newId;
    }

    public TableHandle getSource() {
        return source;
    }

    public TableHandle getNewId() {
        return newId;
    }

    void setSource(TableHandle source) {
        this.source = source;
    }

    void setNewId(TableHandle newId) {
        this.newId = newId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final HandleMapping that = (HandleMapping) o;

        if (!source.equals(that.source))
            return false;
        return newId.equals(that.newId);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + newId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "HandleMapping{" +
            "source=" + source +
            ", newId=" + newId +
            '}';
    }
}
