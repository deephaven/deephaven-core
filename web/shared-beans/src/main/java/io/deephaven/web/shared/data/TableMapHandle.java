package io.deephaven.web.shared.data;

import java.io.Serializable;

public class TableMapHandle implements Serializable {
    private int serverId;

    public TableMapHandle() {}

    public TableMapHandle(int serverId) {
        setServerId(serverId);
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TableMapHandle that = (TableMapHandle) o;

        return serverId == that.serverId;
    }

    @Override
    public int hashCode() {
        return serverId;
    }
}
