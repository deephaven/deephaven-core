package io.deephaven.web.shared.data;

import java.io.Serializable;

public class HeadOrTailDescriptor implements Serializable {

    private boolean head;
    private long rows;

    public HeadOrTailDescriptor(boolean head, long rows) {
        this.head = head;
        this.rows = rows;
    }

    public HeadOrTailDescriptor() {}

    public boolean isHead() {
        return head;
    }

    public void setHead(boolean head) {
        this.head = head;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "HeadOrTailDescriptor{" +
            "head=" + head +
            ", rows=" + rows +
            '}';
    }
}
